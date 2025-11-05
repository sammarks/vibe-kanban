#[cfg(test)]
mod filesystem_watcher_tests {
    use std::{fs, path::Path, time::Duration};

    use futures::StreamExt;
    use services::services::filesystem_watcher::async_watcher;
    use tempfile::TempDir;

    /// Helper function to create a directory structure
    fn create_dir_structure(base: &Path, path: &str) {
        let full_path = base.join(path);
        if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::create_dir_all(&full_path).unwrap();
    }

    /// Helper function to create a file
    fn create_file(base: &Path, path: &str, content: &str) {
        let full_path = base.join(path);
        if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(&full_path, content).unwrap();
    }

    #[tokio::test]
    async fn test_watcher_excludes_node_modules() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path();

        // Create test structure with node_modules
        create_dir_structure(base_path, "src");
        create_dir_structure(base_path, "node_modules/some-package");
        create_dir_structure(base_path, "node_modules/.pnpm/some-package@1.0.0");

        // Initialize watcher
        let (debouncer, mut rx, _canonical_root) =
            async_watcher(base_path.to_path_buf()).unwrap();

        // Create files in both src and node_modules
        create_file(base_path, "src/main.js", "console.log('main');");
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Check for src file event
        let mut src_event_found = false;
        while let Ok(Some(result)) = tokio::time::timeout(Duration::from_millis(100), rx.next()).await
        {
            if let Ok(events) = result {
                for event in events {
                    for path in &event.paths {
                        if path.to_string_lossy().contains("src/main.js") {
                            src_event_found = true;
                        }
                        // Ensure no node_modules events
                        assert!(
                            !path.to_string_lossy().contains("node_modules"),
                            "Watcher should not watch node_modules, but got event for: {path:?}"
                        );
                    }
                }
            }
        }

        assert!(src_event_found, "Should detect src file changes");

        // Now create a file in node_modules and ensure it's not watched
        create_file(
            base_path,
            "node_modules/some-package/index.js",
            "module.exports = {};",
        );
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Check that we don't get events from node_modules
        let mut node_modules_event_found = false;
        while let Ok(Some(result)) = tokio::time::timeout(Duration::from_millis(100), rx.next()).await
        {
            if let Ok(events) = result {
                for event in events {
                    for path in &event.paths {
                        if path.to_string_lossy().contains("node_modules") {
                            node_modules_event_found = true;
                        }
                    }
                }
            }
        }

        assert!(
            !node_modules_event_found,
            "Watcher should not watch node_modules"
        );

        // Clean up
        drop(debouncer);
    }

    #[tokio::test]
    async fn test_watcher_excludes_build_directories() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path();

        // Create test structure with various build directories
        create_dir_structure(base_path, "src");
        create_dir_structure(base_path, "target/debug");
        create_dir_structure(base_path, "dist");
        create_dir_structure(base_path, "build");
        create_dir_structure(base_path, ".next/cache");

        // Initialize watcher
        let (debouncer, mut rx, _canonical_root) =
            async_watcher(base_path.to_path_buf()).unwrap();

        // Create files in src
        create_file(base_path, "src/lib.rs", "fn main() {}");
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Check for src file event
        let mut src_event_found = false;
        while let Ok(Some(result)) = tokio::time::timeout(Duration::from_millis(100), rx.next()).await
        {
            if let Ok(events) = result {
                for event in events {
                    for path in &event.paths {
                        if path.to_string_lossy().contains("src/lib.rs") {
                            src_event_found = true;
                        }
                        // Ensure no build directory events
                        let path_str = path.to_string_lossy();
                        assert!(
                            !path_str.contains("target/")
                                && !path_str.contains("dist/")
                                && !path_str.contains("build/")
                                && !path_str.contains(".next/"),
                            "Watcher should not watch build directories, but got event for: {path:?}"
                        );
                    }
                }
            }
        }

        assert!(src_event_found, "Should detect src file changes");

        // Create files in build directories and ensure they're not watched
        create_file(base_path, "target/debug/app", "binary");
        create_file(base_path, "dist/bundle.js", "bundled code");
        create_file(base_path, "build/output.js", "build output");
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Check that we don't get events from build directories
        let mut build_event_found = false;
        while let Ok(Some(result)) = tokio::time::timeout(Duration::from_millis(100), rx.next()).await
        {
            if let Ok(events) = result {
                for event in events {
                    for path in &event.paths {
                        let path_str = path.to_string_lossy();
                        if path_str.contains("target/")
                            || path_str.contains("dist/")
                            || path_str.contains("build/")
                        {
                            build_event_found = true;
                        }
                    }
                }
            }
        }

        assert!(
            !build_event_found,
            "Watcher should not watch build directories"
        );

        // Clean up
        drop(debouncer);
    }

    #[tokio::test]
    async fn test_watcher_watches_normal_directories() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path();

        // Create normal directory structure
        create_dir_structure(base_path, "src");
        create_dir_structure(base_path, "tests");
        create_dir_structure(base_path, "docs");

        // Initialize watcher
        let (debouncer, mut rx, _canonical_root) =
            async_watcher(base_path.to_path_buf()).unwrap();

        // Create files in various directories
        create_file(base_path, "src/main.rs", "fn main() {}");
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Collect events
        let mut events_by_path = std::collections::HashSet::new();
        while let Ok(Some(result)) = tokio::time::timeout(Duration::from_millis(100), rx.next()).await
        {
            if let Ok(events) = result {
                for event in events {
                    for path in &event.paths {
                        events_by_path.insert(path.to_string_lossy().to_string());
                    }
                }
            }
        }

        // Should have detected the src file
        assert!(
            events_by_path
                .iter()
                .any(|p| p.contains("src/main.rs")),
            "Should detect src file changes"
        );

        // Create more files
        create_file(base_path, "tests/test.rs", "test code");
        create_file(base_path, "docs/README.md", "documentation");
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Collect more events
        while let Ok(Some(result)) = tokio::time::timeout(Duration::from_millis(100), rx.next()).await
        {
            if let Ok(events) = result {
                for event in events {
                    for path in &event.paths {
                        events_by_path.insert(path.to_string_lossy().to_string());
                    }
                }
            }
        }

        // Should have detected changes in normal directories
        assert!(
            events_by_path.iter().any(|p| p.contains("tests/test.rs"))
                || events_by_path.iter().any(|p| p.contains("docs/README.md")),
            "Should detect changes in normal directories"
        );

        // Clean up
        drop(debouncer);
    }
}
