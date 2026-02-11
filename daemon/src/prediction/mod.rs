//! Prediction engine for command suggestions.
//!
//! Combines n-gram models with context-aware ranking for fast (<10ms) predictions.

mod ngram;
pub mod parser;
mod ranking;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::protocol::Suggestion;

/// LRU cache for predictions
#[allow(dead_code)]
struct PredictionCache {
    entries: HashMap<String, CacheEntry>,
    max_size: usize,
}

#[allow(dead_code)]
struct CacheEntry {
    suggestions: Vec<Suggestion>,
    timestamp: Instant,
}

#[allow(dead_code)]
impl PredictionCache {
    fn new(max_size: usize) -> Self {
        Self {
            entries: HashMap::new(),
            max_size,
        }
    }

    fn get(&self, key: &str) -> Option<&Vec<Suggestion>> {
        self.entries.get(key).map(|e| &e.suggestions)
    }

    fn insert(&mut self, key: String, suggestions: Vec<Suggestion>) {
        // Simple eviction: remove oldest entries if over capacity
        if self.entries.len() >= self.max_size {
            let oldest = self
                .entries
                .iter()
                .min_by_key(|(_, v)| v.timestamp)
                .map(|(k, _)| k.clone());

            if let Some(k) = oldest {
                self.entries.remove(&k);
            }
        }

        self.entries.insert(
            key,
            CacheEntry {
                suggestions,
                timestamp: Instant::now(),
            },
        );
    }

    fn invalidate_prefix(&mut self, prefix: &str) {
        self.entries.retain(|k, _| !k.starts_with(prefix));
    }
}

/// Prediction engine combining n-gram model and context ranking
#[allow(dead_code)]
pub struct PredictionEngine {
    cache: Arc<Mutex<PredictionCache>>,
}

impl Default for PredictionEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(dead_code)]
impl PredictionEngine {
    pub fn new() -> Self {
        Self {
            cache: Arc::new(Mutex::new(PredictionCache::new(1000))),
        }
    }

    /// Generate cache key from prediction parameters
    fn cache_key(prefix: &str, cwd: &str, last_cmd: Option<&str>) -> String {
        format!(
            "{}:{}:{}",
            prefix,
            cwd,
            last_cmd.unwrap_or("")
        )
    }

    /// Check cache for existing predictions
    pub fn get_cached(&self, prefix: &str, cwd: &str, last_cmd: Option<&str>) -> Option<Vec<Suggestion>> {
        let key = Self::cache_key(prefix, cwd, last_cmd);
        let cache = self.cache.lock().unwrap();
        cache.get(&key).cloned()
    }

    /// Store predictions in cache
    pub fn cache_predictions(&self, prefix: &str, cwd: &str, last_cmd: Option<&str>, suggestions: Vec<Suggestion>) {
        let key = Self::cache_key(prefix, cwd, last_cmd);
        let mut cache = self.cache.lock().unwrap();
        cache.insert(key, suggestions);
    }

    /// Invalidate cache entries matching a prefix pattern
    pub fn invalidate_cache(&self, prefix: &str) {
        let mut cache = self.cache.lock().unwrap();
        cache.invalidate_prefix(prefix);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prediction_cache() {
        let engine = PredictionEngine::new();

        let suggestions = vec![
            Suggestion {
                cmd: "git status".to_string(),
                score: 0.9,
            },
            Suggestion {
                cmd: "git add".to_string(),
                score: 0.8,
            },
        ];

        // Cache miss
        assert!(engine.get_cached("git", "/home/user", None).is_none());

        // Cache hit after insert
        engine.cache_predictions("git", "/home/user", None, suggestions.clone());
        let cached = engine.get_cached("git", "/home/user", None);
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().len(), 2);

        // Different context = cache miss
        assert!(engine.get_cached("git", "/other/dir", None).is_none());
    }

    #[test]
    fn test_cache_eviction() {
        let mut cache = PredictionCache::new(2);

        cache.insert("a".to_string(), vec![]);
        cache.insert("b".to_string(), vec![]);
        assert_eq!(cache.entries.len(), 2);

        // Third insert should evict oldest
        cache.insert("c".to_string(), vec![]);
        assert_eq!(cache.entries.len(), 2);
        assert!(cache.entries.contains_key("c"));
    }
}
