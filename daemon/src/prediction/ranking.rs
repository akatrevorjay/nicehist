//! Context-aware ranking for command predictions.
//!
//! Combines n-gram scores with contextual signals:
//! - Directory frequency (commands often used in this directory)
//! - Project type match (commands typical for this project type)
//! - Recency decay (recent commands ranked higher)
//! - Time-of-day patterns (optional)

#![allow(dead_code)]

use std::time::{SystemTime, UNIX_EPOCH};

/// Context information for ranking
#[derive(Debug, Clone, Default)]
pub struct RankingContext {
    /// Current working directory
    pub cwd: String,
    /// Detected project type (rust, node, python, etc.)
    pub project_type: Option<String>,
    /// VCS branch name
    pub vcs_branch: Option<String>,
    /// Hour of day (0-23)
    pub hour: Option<u8>,
}

impl RankingContext {
    pub fn new(cwd: String) -> Self {
        Self {
            cwd,
            project_type: None,
            vcs_branch: None,
            hour: None,
        }
    }

    pub fn with_project(mut self, project: Option<String>) -> Self {
        self.project_type = project;
        self
    }

    pub fn with_branch(mut self, branch: Option<String>) -> Self {
        self.vcs_branch = branch;
        self
    }

    pub fn with_hour(mut self, hour: u8) -> Self {
        self.hour = Some(hour);
        self
    }
}

/// Context-aware ranker
pub struct ContextRanker;

impl ContextRanker {
    /// Calculate context score for a command
    ///
    /// Returns a score between 0.0 and 1.0 based on:
    /// - Directory frequency bonus
    /// - Project type match
    /// - VCS branch pattern match
    /// - Time-of-day patterns
    pub fn context_score(
        cmd: &str,
        context: &RankingContext,
        dir_frequency: i64,
        total_in_dir: i64,
    ) -> f64 {
        let mut score = 0.0;

        // Directory frequency bonus (0.0 - 0.30)
        if total_in_dir > 0 {
            let dir_ratio = dir_frequency as f64 / total_in_dir as f64;
            score += 0.30 * dir_ratio.min(1.0);
        }

        // Project type match (0.0 - 0.20)
        if let Some(ref project) = context.project_type {
            if Self::matches_project_type(cmd, project) {
                score += 0.20;
            }
        }

        // VCS branch pattern (0.0 - 0.15)
        if let Some(ref branch) = context.vcs_branch {
            if Self::matches_branch_pattern(cmd, branch) {
                score += 0.15;
            }
        }

        score.min(1.0)
    }

    /// Check if command matches typical commands for a project type
    fn matches_project_type(cmd: &str, project: &str) -> bool {
        let cmd_lower = cmd.to_lowercase();

        match project {
            "rust" => {
                cmd_lower.starts_with("cargo ")
                    || cmd_lower.starts_with("rustc ")
                    || cmd_lower.starts_with("rustup ")
            }
            "node" => {
                cmd_lower.starts_with("npm ")
                    || cmd_lower.starts_with("yarn ")
                    || cmd_lower.starts_with("pnpm ")
                    || cmd_lower.starts_with("node ")
                    || cmd_lower.starts_with("npx ")
            }
            "python" => {
                cmd_lower.starts_with("python")
                    || cmd_lower.starts_with("pip ")
                    || cmd_lower.starts_with("pytest")
                    || cmd_lower.starts_with("poetry ")
                    || cmd_lower.starts_with("pdm ")
            }
            "go" => {
                cmd_lower.starts_with("go ")
            }
            "ruby" => {
                cmd_lower.starts_with("ruby ")
                    || cmd_lower.starts_with("bundle ")
                    || cmd_lower.starts_with("rake ")
                    || cmd_lower.starts_with("rails ")
            }
            "java" => {
                cmd_lower.starts_with("mvn ")
                    || cmd_lower.starts_with("gradle ")
                    || cmd_lower.starts_with("java ")
            }
            _ => false,
        }
    }

    /// Check if command matches patterns for a VCS branch
    fn matches_branch_pattern(cmd: &str, branch: &str) -> bool {
        let cmd_lower = cmd.to_lowercase();
        let branch_lower = branch.to_lowercase();

        // Feature branches often involve specific commands
        if branch_lower.starts_with("feature/") || branch_lower.starts_with("feat/") {
            // New feature development
            return cmd_lower.contains("test") || cmd_lower.contains("build");
        }

        if branch_lower.starts_with("fix/") || branch_lower.starts_with("bugfix/") {
            // Bug fixing often involves debugging
            return cmd_lower.contains("test") || cmd_lower.contains("debug");
        }

        if branch_lower == "main" || branch_lower == "master" {
            // Main branch often involves deployment
            return cmd_lower.contains("deploy")
                || cmd_lower.contains("release")
                || cmd_lower.contains("push");
        }

        false
    }

    /// Calculate recency decay factor
    ///
    /// Exponential decay: exp(-age_days / half_life_days)
    pub fn recency_decay(last_used_timestamp: i64, half_life_days: f64) -> f64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        let age_seconds = (now - last_used_timestamp).max(0);
        let age_days = age_seconds as f64 / 86400.0;

        (-age_days / half_life_days).exp()
    }

    /// Combine n-gram score with context score
    ///
    /// Formula: final = ngram_score * 0.6 + context_score * 0.4
    pub fn combined_score(ngram_score: f64, context_score: f64, recency: f64) -> f64 {
        const NGRAM_WEIGHT: f64 = 0.50;
        const CONTEXT_WEIGHT: f64 = 0.30;
        const RECENCY_WEIGHT: f64 = 0.20;

        let score = NGRAM_WEIGHT * ngram_score
            + CONTEXT_WEIGHT * context_score
            + RECENCY_WEIGHT * recency;

        score.min(1.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_matches_project_type_rust() {
        assert!(ContextRanker::matches_project_type("cargo build", "rust"));
        assert!(ContextRanker::matches_project_type("cargo test", "rust"));
        assert!(!ContextRanker::matches_project_type("npm install", "rust"));
    }

    #[test]
    fn test_matches_project_type_node() {
        assert!(ContextRanker::matches_project_type("npm install", "node"));
        assert!(ContextRanker::matches_project_type("yarn add", "node"));
        assert!(!ContextRanker::matches_project_type("cargo build", "node"));
    }

    #[test]
    fn test_recency_decay() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        // Just now should be ~1.0
        let recent = ContextRanker::recency_decay(now, 30.0);
        assert!(recent > 0.95);

        // 30 days ago should be ~0.37 (1/e)
        let old = ContextRanker::recency_decay(now - 30 * 86400, 30.0);
        assert!(old > 0.30 && old < 0.45);

        // Very old should approach 0
        let ancient = ContextRanker::recency_decay(now - 365 * 86400, 30.0);
        assert!(ancient < 0.01);
    }

    #[test]
    fn test_combined_score() {
        let score = ContextRanker::combined_score(0.8, 0.6, 1.0);
        assert!(score > 0.5);
        assert!(score <= 1.0);

        // Higher ngram and context should give higher score
        let higher = ContextRanker::combined_score(1.0, 1.0, 1.0);
        assert!(higher > score);
    }

    #[test]
    fn test_context_score() {
        let ctx = RankingContext::new("/home/user/project".to_string())
            .with_project(Some("rust".to_string()));

        // Rust command in rust project should have high context score
        let score = ContextRanker::context_score("cargo build", &ctx, 10, 20);
        assert!(score > 0.0);

        // Non-matching command should have lower score
        let score2 = ContextRanker::context_score("npm install", &ctx, 0, 20);
        assert!(score2 < score);
    }
}
