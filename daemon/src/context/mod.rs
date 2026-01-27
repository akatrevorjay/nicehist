//! Context collection for directories.
//!
//! Detects VCS (git, hg) and project type (rust, node, python, etc.)
//! for context-aware command predictions.

mod project;
mod vcs;

pub use project::{detect_project_type, ProjectType};
pub use vcs::{detect_vcs, VcsInfo};

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::protocol::ContextInfo;

/// Context cache entry
struct CacheEntry {
    info: ContextInfo,
    timestamp: Instant,
}

/// Context collector with caching
pub struct ContextCollector {
    cache: Arc<Mutex<HashMap<String, CacheEntry>>>,
    cache_ttl: Duration,
}

impl Default for ContextCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl ContextCollector {
    /// Create a new context collector with default TTL (5 seconds)
    pub fn new() -> Self {
        Self {
            cache: Arc::new(Mutex::new(HashMap::new())),
            cache_ttl: Duration::from_secs(5),
        }
    }

    /// Create with custom cache TTL
    pub fn with_ttl(ttl: Duration) -> Self {
        Self {
            cache: Arc::new(Mutex::new(HashMap::new())),
            cache_ttl: ttl,
        }
    }

    /// Get context for a directory (cached)
    pub fn get_context(&self, dir: &str) -> ContextInfo {
        // Check cache first
        {
            let cache = self.cache.lock().unwrap();
            if let Some(entry) = cache.get(dir) {
                if entry.timestamp.elapsed() < self.cache_ttl {
                    return entry.info.clone();
                }
            }
        }

        // Compute fresh context
        let info = self.compute_context(dir);

        // Update cache
        {
            let mut cache = self.cache.lock().unwrap();
            cache.insert(
                dir.to_string(),
                CacheEntry {
                    info: info.clone(),
                    timestamp: Instant::now(),
                },
            );
        }

        info
    }

    /// Invalidate cache for a directory
    pub fn invalidate(&self, dir: &str) {
        let mut cache = self.cache.lock().unwrap();
        cache.remove(dir);
    }

    /// Compute context without caching
    fn compute_context(&self, dir: &str) -> ContextInfo {
        let path = Path::new(dir);

        // Detect VCS
        let vcs_info = detect_vcs(path);

        // Detect project type
        let project_type = detect_project_type(path);

        ContextInfo {
            vcs: vcs_info.as_ref().map(|v| v.vcs_type.to_string()),
            branch: vcs_info.as_ref().and_then(|v| v.branch.clone()),
            vcs_root: vcs_info.map(|v| v.root.to_string_lossy().to_string()),
            project: project_type.map(|p| p.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_context_collector_caching() {
        let collector = ContextCollector::new();

        // Get context twice - second should be cached
        let ctx1 = collector.get_context("/tmp");
        let ctx2 = collector.get_context("/tmp");

        // Both should return same result
        assert_eq!(ctx1.vcs, ctx2.vcs);
        assert_eq!(ctx1.project, ctx2.project);
    }

    #[test]
    fn test_context_collector_invalidate() {
        let collector = ContextCollector::new();

        // Get context
        let _ctx1 = collector.get_context("/tmp");

        // Invalidate
        collector.invalidate("/tmp");

        // Cache should be empty for that dir
        let cache = collector.cache.lock().unwrap();
        assert!(!cache.contains_key("/tmp"));
    }

    #[test]
    fn test_context_for_current_dir() {
        let collector = ContextCollector::new();
        let cwd = env::current_dir().unwrap();
        let ctx = collector.get_context(cwd.to_str().unwrap());

        // Current directory (nicehist) should be detected as a git repo
        // and a Rust project
        if cwd.join(".git").exists() || cwd.join("Cargo.toml").exists() {
            // We expect at least one of these to be detected
            assert!(ctx.vcs.is_some() || ctx.project.is_some());
        }
    }
}
