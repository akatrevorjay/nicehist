//! Project type detection.
//!
//! Detects project type based on manifest files (Cargo.toml, package.json, etc.)

use std::fmt;
use std::path::Path;

/// Detected project type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectType {
    Rust,
    Node,
    Python,
    Go,
    Ruby,
    Java,
    CSharp,
    Cpp,
    C,
    Php,
    Elixir,
    Haskell,
    Scala,
    Kotlin,
    Swift,
    Zig,
}

impl fmt::Display for ProjectType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ProjectType::Rust => "rust",
            ProjectType::Node => "node",
            ProjectType::Python => "python",
            ProjectType::Go => "go",
            ProjectType::Ruby => "ruby",
            ProjectType::Java => "java",
            ProjectType::CSharp => "csharp",
            ProjectType::Cpp => "cpp",
            ProjectType::C => "c",
            ProjectType::Php => "php",
            ProjectType::Elixir => "elixir",
            ProjectType::Haskell => "haskell",
            ProjectType::Scala => "scala",
            ProjectType::Kotlin => "kotlin",
            ProjectType::Swift => "swift",
            ProjectType::Zig => "zig",
        };
        write!(f, "{}", s)
    }
}

/// Manifest files and their associated project types
const PROJECT_MARKERS: &[(&str, ProjectType)] = &[
    // Rust
    ("Cargo.toml", ProjectType::Rust),
    // Node.js
    ("package.json", ProjectType::Node),
    // Python
    ("pyproject.toml", ProjectType::Python),
    ("setup.py", ProjectType::Python),
    ("setup.cfg", ProjectType::Python),
    ("requirements.txt", ProjectType::Python),
    ("Pipfile", ProjectType::Python),
    // Go
    ("go.mod", ProjectType::Go),
    ("go.sum", ProjectType::Go),
    // Ruby
    ("Gemfile", ProjectType::Ruby),
    ("Rakefile", ProjectType::Ruby),
    // Java
    ("pom.xml", ProjectType::Java),
    ("build.gradle", ProjectType::Java),
    ("build.gradle.kts", ProjectType::Kotlin),
    // C#
    ("*.csproj", ProjectType::CSharp),
    ("*.sln", ProjectType::CSharp),
    // C/C++
    ("CMakeLists.txt", ProjectType::Cpp),
    ("Makefile", ProjectType::C),
    ("meson.build", ProjectType::Cpp),
    // PHP
    ("composer.json", ProjectType::Php),
    // Elixir
    ("mix.exs", ProjectType::Elixir),
    // Haskell
    ("stack.yaml", ProjectType::Haskell),
    ("*.cabal", ProjectType::Haskell),
    // Scala
    ("build.sbt", ProjectType::Scala),
    // Kotlin
    ("build.gradle.kts", ProjectType::Kotlin),
    // Swift
    ("Package.swift", ProjectType::Swift),
    // Zig
    ("build.zig", ProjectType::Zig),
];

/// Detect project type for a directory
///
/// Looks for manifest files in the directory and walks up if needed.
/// Returns the first matching project type.
pub fn detect_project_type(path: &Path) -> Option<ProjectType> {
    let mut current = if path.is_file() {
        path.parent()?.to_path_buf()
    } else {
        path.to_path_buf()
    };

    // Try current directory first
    if let Some(pt) = detect_in_dir(&current) {
        return Some(pt);
    }

    // Walk up to find project root (max 10 levels)
    for _ in 0..10 {
        if !current.pop() {
            break;
        }
        if let Some(pt) = detect_in_dir(&current) {
            return Some(pt);
        }
    }

    None
}

/// Check for project markers in a specific directory
fn detect_in_dir(dir: &Path) -> Option<ProjectType> {
    for (marker, project_type) in PROJECT_MARKERS {
        if marker.starts_with('*') {
            // Glob pattern - check for any matching file
            let ext = &marker[1..]; // e.g., ".csproj"
            if let Ok(entries) = std::fs::read_dir(dir) {
                for entry in entries.flatten() {
                    if let Some(name) = entry.file_name().to_str() {
                        if name.ends_with(ext) {
                            return Some(*project_type);
                        }
                    }
                }
            }
        } else {
            // Exact file name
            if dir.join(marker).exists() {
                return Some(*project_type);
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_project_type_display() {
        assert_eq!(ProjectType::Rust.to_string(), "rust");
        assert_eq!(ProjectType::Node.to_string(), "node");
        assert_eq!(ProjectType::Python.to_string(), "python");
    }

    #[test]
    fn test_detect_rust_project() {
        let cwd = env::current_dir().unwrap();
        if cwd.join("Cargo.toml").exists() {
            let pt = detect_project_type(&cwd);
            assert_eq!(pt, Some(ProjectType::Rust));
        }
    }

    #[test]
    fn test_detect_from_subdir() {
        let cwd = env::current_dir().unwrap();
        if cwd.join("Cargo.toml").exists() {
            let subdir = cwd.join("daemon/src");
            if subdir.exists() {
                let pt = detect_project_type(&subdir);
                assert_eq!(pt, Some(ProjectType::Rust));
            }
        }
    }

    #[test]
    fn test_detect_in_dir() {
        let cwd = env::current_dir().unwrap();
        if cwd.join("Cargo.toml").exists() {
            let pt = detect_in_dir(&cwd);
            assert_eq!(pt, Some(ProjectType::Rust));
        }
    }

    #[test]
    fn test_detect_nonexistent() {
        // /tmp is unlikely to have project markers
        let pt = detect_project_type(Path::new("/tmp"));
        // Could be None or could detect something if /tmp is in a project
        // Just make sure it doesn't crash
        let _ = pt;
    }
}
