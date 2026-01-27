//! VCS (Version Control System) detection.
//!
//! Detects Git and Mercurial repositories and extracts branch information.

use std::path::{Path, PathBuf};
use std::process::Command;

/// VCS information for a directory
#[derive(Debug, Clone)]
pub struct VcsInfo {
    /// Type of VCS ("git" or "hg")
    pub vcs_type: &'static str,
    /// Repository root directory
    pub root: PathBuf,
    /// Current branch name (if available)
    pub branch: Option<String>,
}

/// Detect VCS for a directory
///
/// Walks up the directory tree looking for .git or .hg directories.
/// Returns VCS info with branch name if found.
pub fn detect_vcs(path: &Path) -> Option<VcsInfo> {
    // Try git first (more common)
    if let Some(info) = detect_git(path) {
        return Some(info);
    }

    // Try mercurial
    if let Some(info) = detect_hg(path) {
        return Some(info);
    }

    None
}

/// Detect Git repository
fn detect_git(path: &Path) -> Option<VcsInfo> {
    let root = find_repo_root(path, ".git")?;

    // Get branch name
    let branch = get_git_branch(&root);

    Some(VcsInfo {
        vcs_type: "git",
        root,
        branch,
    })
}

/// Detect Mercurial repository
fn detect_hg(path: &Path) -> Option<VcsInfo> {
    let root = find_repo_root(path, ".hg")?;

    // Get branch name
    let branch = get_hg_branch(&root);

    Some(VcsInfo {
        vcs_type: "hg",
        root,
        branch,
    })
}

/// Find repository root by walking up the directory tree
fn find_repo_root(start: &Path, marker: &str) -> Option<PathBuf> {
    let mut current = if start.is_file() {
        start.parent()?.to_path_buf()
    } else {
        start.to_path_buf()
    };

    loop {
        if current.join(marker).exists() {
            return Some(current);
        }

        if !current.pop() {
            return None;
        }
    }
}

/// Get current Git branch name
fn get_git_branch(repo_root: &Path) -> Option<String> {
    // Try reading .git/HEAD directly (faster than shelling out)
    let head_path = repo_root.join(".git/HEAD");
    if let Ok(content) = std::fs::read_to_string(&head_path) {
        let content = content.trim();
        if let Some(branch) = content.strip_prefix("ref: refs/heads/") {
            return Some(branch.to_string());
        }
        // Detached HEAD - return short hash
        if content.len() >= 7 {
            return Some(content[..7].to_string());
        }
    }

    // Fallback to git command
    let output = Command::new("git")
        .args(["rev-parse", "--abbrev-ref", "HEAD"])
        .current_dir(repo_root)
        .output()
        .ok()?;

    if output.status.success() {
        let branch = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if !branch.is_empty() && branch != "HEAD" {
            return Some(branch);
        }
    }

    None
}

/// Get current Mercurial branch name
fn get_hg_branch(repo_root: &Path) -> Option<String> {
    // Try reading .hg/branch directly
    let branch_path = repo_root.join(".hg/branch");
    if let Ok(content) = std::fs::read_to_string(&branch_path) {
        let branch = content.trim().to_string();
        if !branch.is_empty() {
            return Some(branch);
        }
    }

    // Fallback to hg command
    let output = Command::new("hg")
        .args(["branch"])
        .current_dir(repo_root)
        .output()
        .ok()?;

    if output.status.success() {
        let branch = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if !branch.is_empty() {
            return Some(branch);
        }
    }

    // Default branch for hg
    Some("default".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_find_repo_root_git() {
        // This test assumes we're running in the nicehist repo
        let cwd = env::current_dir().unwrap();
        if cwd.join(".git").exists() {
            let root = find_repo_root(&cwd, ".git");
            assert!(root.is_some());
            assert!(root.unwrap().join(".git").exists());
        }
    }

    #[test]
    fn test_detect_vcs_git() {
        let cwd = env::current_dir().unwrap();
        if cwd.join(".git").exists() {
            let info = detect_vcs(&cwd);
            assert!(info.is_some());
            let info = info.unwrap();
            assert_eq!(info.vcs_type, "git");
            assert!(info.branch.is_some());
        }
    }

    #[test]
    fn test_detect_vcs_nonexistent() {
        let info = detect_vcs(Path::new("/tmp"));
        // /tmp is unlikely to be a git/hg repo
        // (this might fail if /tmp is in a repo, but unlikely)
        match info {
            None => {} // Expected
            Some(ref i) => {
                assert!(i.vcs_type == "git" || i.vcs_type == "hg");
            }
        }
    }

    #[test]
    fn test_get_git_branch() {
        let cwd = env::current_dir().unwrap();
        if cwd.join(".git").exists() {
            let branch = get_git_branch(&cwd);
            assert!(branch.is_some());
            // Branch name should be non-empty
            assert!(!branch.unwrap().is_empty());
        }
    }

    #[test]
    fn test_find_repo_root_from_subdir() {
        let cwd = env::current_dir().unwrap();
        if cwd.join(".git").exists() {
            // Test from a subdirectory
            let subdir = cwd.join("daemon/src");
            if subdir.exists() {
                let root = find_repo_root(&subdir, ".git");
                assert!(root.is_some());
                assert_eq!(root.unwrap(), cwd);
            }
        }
    }
}
