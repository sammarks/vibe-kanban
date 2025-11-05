use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use futures::{
    SinkExt,
    channel::mpsc::{Receiver, channel},
};
use ignore::{
    WalkBuilder,
    gitignore::{Gitignore, GitignoreBuilder},
};
use notify::{RecommendedWatcher, RecursiveMode};
use notify_debouncer_full::{
    DebounceEventResult, DebouncedEvent, Debouncer, RecommendedCache, new_debouncer,
};
use thiserror::Error;

pub type WatcherComponents = (
    Debouncer<RecommendedWatcher, RecommendedCache>,
    Receiver<DebounceEventResult>,
    PathBuf,
);

#[derive(Debug, Error)]
pub enum FilesystemWatcherError {
    #[error(transparent)]
    Notify(#[from] notify::Error),
    #[error(transparent)]
    Ignore(#[from] ignore::Error),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("Failed to build gitignore: {0}")]
    GitignoreBuilder(String),
    #[error("Invalid path: {0}")]
    InvalidPath(String),
}

fn canonicalize_lossy(path: &Path) -> PathBuf {
    dunce::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

/// Directories that should always be excluded from filesystem watching
/// to prevent memory leaks from large dependency and build directories.
/// This is especially important for PNPM node_modules with extensive symlink structures.
const EXCLUDED_DIRS: &[&str] = &[
    "node_modules",
    ".pnpm",
    "target",
    "dist",
    "build",
    ".next",
    ".nuxt",
];

/// Check if a directory name should be excluded from watching
fn is_excluded_dir(name: &str) -> bool {
    EXCLUDED_DIRS.contains(&name)
}

fn build_gitignore_set(root: &Path) -> Result<Gitignore, FilesystemWatcherError> {
    let mut builder = GitignoreBuilder::new(root);

    // Walk once to collect all .gitignore files under root
    WalkBuilder::new(root)
        .follow_links(false)
        .hidden(false) // we *want* to see .gitignore
        .filter_entry(|entry| {
            // Skip common dependency and build directories to prevent memory leaks
            if let Some(name) = entry.file_name().to_str()
                && is_excluded_dir(name)
            {
                return false;
            }

            // only recurse into directories and .gitignore files
            entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false)
                || entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name == ".gitignore")
        })
        .build()
        .try_for_each(|result| {
            // everything that is not a directory and is named .gitignore
            match result {
                Ok(dir_entry) => {
                    if !dir_entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
                        builder.add(dir_entry.path());
                    }
                    Ok(())
                }
                Err(err)
                    if err.io_error().is_some_and(|io_err| {
                        io_err.kind() == std::io::ErrorKind::PermissionDenied
                    }) =>
                {
                    // Skip entries we don't have permission to read
                    tracing::warn!("Permission denied reading path: {}", err);
                    Ok(())
                }
                Err(e) => Err(FilesystemWatcherError::Ignore(e)),
            }
        })?;

    // Optionally include repo-local excludes
    let info_exclude = root.join(".git/info/exclude");
    if info_exclude.exists() {
        builder.add(info_exclude);
    }

    Ok(builder.build()?)
}

fn path_allowed(path: &Path, gi: &Gitignore, canonical_root: &Path) -> bool {
    let canonical_path = canonicalize_lossy(path);

    // Check for excluded directories that should always be filtered out
    for component in canonical_path.components() {
        if let Some(name) = component.as_os_str().to_str()
            && is_excluded_dir(name)
        {
            return false;
        }
    }

    // Convert absolute path to relative path from the gitignore root
    let relative_path = match canonical_path.strip_prefix(canonical_root) {
        Ok(rel_path) => rel_path,
        Err(_) => {
            // Path is outside the watched root, don't ignore it
            return true;
        }
    };

    // Heuristic: assume paths without extensions are directories
    // This works for most cases and avoids filesystem syscalls
    let is_dir = relative_path.extension().is_none();
    let matched = gi.matched_path_or_any_parents(relative_path, is_dir);

    !matched.is_ignore()
}

fn debounced_should_forward(event: &DebouncedEvent, gi: &Gitignore, canonical_root: &Path) -> bool {
    // DebouncedEvent is a struct that wraps the underlying notify::Event
    if event.kind.is_access() {
        // Ignore access events
        return false;
    }
    // We can check its paths field to determine if the event should be forwarded
    event
        .paths
        .iter()
        .all(|path| path_allowed(path, gi, canonical_root))
}

/// Efficiently set up selective watches for directories, excluding node_modules and build dirs
fn setup_selective_watches(
    debouncer: &mut Debouncer<RecommendedWatcher, RecommendedCache>,
    root: &Path,
) -> Result<(), FilesystemWatcherError> {
    // Start by watching the root directory with NonRecursive mode
    debouncer
        .watch(root, RecursiveMode::NonRecursive)
        .map_err(FilesystemWatcherError::Notify)?;

    // Use a simple BFS traversal with excluded directory filtering
    // This is more efficient than deep recursion and allows us to skip entire subtrees
    let mut dirs_to_process = vec![root.to_path_buf()];
    let mut watched_count = 1; // Root is already watched

    while let Some(dir) = dirs_to_process.pop() {
        // Read directory entries
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) => {
                tracing::debug!("Failed to read directory {:?}: {}", dir, e);
                continue;
            }
        };

        for entry in entries.flatten() {
            let path = entry.path();
            
            // Only process directories
            if !path.is_dir() {
                continue;
            }

            // Check if this directory should be excluded
            if let Some(name) = path.file_name().and_then(|n| n.to_str())
                && is_excluded_dir(name)
            {
                tracing::debug!("Skipping excluded directory: {:?}", path);
                continue; // Skip this entire subtree
            }

            // Add watch for this directory
            match debouncer.watch(&path, RecursiveMode::NonRecursive) {
                Ok(_) => {
                    watched_count += 1;
                    // Add to queue for further processing
                    dirs_to_process.push(path);
                }
                Err(e) => {
                    tracing::debug!("Failed to watch directory {:?}: {}", path, e);
                }
            }
        }
    }

    tracing::info!(
        "Set up {} directory watches for {:?} (excluding: {:?})",
        watched_count,
        root,
        EXCLUDED_DIRS
    );

    Ok(())
}

pub fn async_watcher(root: PathBuf) -> Result<WatcherComponents, FilesystemWatcherError> {
    let canonical_root = canonicalize_lossy(&root);
    
    tracing::debug!("Setting up filesystem watcher for: {:?}", canonical_root);
    
    let gi_set = Arc::new(build_gitignore_set(&canonical_root)?);
    let (mut tx, rx) = channel(64); // Increased capacity for error bursts

    let gi_clone = gi_set.clone();
    let root_clone = canonical_root.clone();

    let mut debouncer = new_debouncer(
        Duration::from_millis(200),
        None, // Use default config
        move |res: DebounceEventResult| {
            match res {
                Ok(events) => {
                    let total_events = events.len();
                    // Filter events and only send allowed ones
                    let filtered_events: Vec<DebouncedEvent> = events
                        .into_iter()
                        .filter(|ev| debounced_should_forward(ev, &gi_clone, &root_clone))
                        .collect();

                    let filtered_count = filtered_events.len();
                    if total_events > filtered_count {
                        tracing::debug!(
                            "Filtered {} of {} events (excluded {} events from node_modules/build dirs)",
                            filtered_count,
                            total_events,
                            total_events - filtered_count
                        );
                    }

                    if !filtered_events.is_empty() {
                        let filtered_result = Ok(filtered_events);
                        futures::executor::block_on(async {
                            tx.send(filtered_result).await.ok();
                        });
                    }
                }
                Err(errors) => {
                    // Always forward errors
                    futures::executor::block_on(async {
                        tx.send(Err(errors)).await.ok();
                    });
                }
            }
        },
    )?;

    // Use selective non-recursive watches to avoid OS-level tracking of excluded directories
    // This prevents memory leaks from node_modules and other large directory trees
    setup_selective_watches(&mut debouncer, &canonical_root)?;

    Ok((debouncer, rx, canonical_root))
}
