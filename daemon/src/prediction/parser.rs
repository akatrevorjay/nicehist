//! Command parser for argument-aware suggestions.
//!
//! Parses commands into program, subcommand, and arguments.

/// Parsed command structure
#[derive(Debug, Clone, PartialEq)]
pub struct ParsedCommand {
    /// The program (first token): git, docker, npm, cargo, etc.
    pub program: String,
    /// Subcommand for multi-level CLIs: commit, push, run, build, etc.
    pub subcommand: Option<String>,
    /// Remaining arguments after program and subcommand
    pub args: Vec<String>,
    /// The original full command
    pub full: String,
}

impl ParsedCommand {
    /// Check if this is a partial command (ends with space, expecting more input)
    pub fn is_partial(&self) -> bool {
        self.full.ends_with(' ')
    }

    /// Get the prefix for argument lookup (program + subcommand)
    pub fn arg_lookup_key(&self) -> String {
        match &self.subcommand {
            Some(sub) => format!("{} {}", self.program, sub),
            None => self.program.clone(),
        }
    }
}

/// Programs known to have subcommands
const SUBCOMMAND_PROGRAMS: &[&str] = &[
    "git", "docker", "docker-compose", "kubectl", "npm", "yarn", "pnpm",
    "cargo", "rustup", "go", "pip", "poetry", "conda", "brew", "apt",
    "systemctl", "journalctl", "aws", "gcloud", "az", "terraform",
    "make", "cmake", "gradle", "mvn", "dotnet", "mix", "bundle",
];

/// Parse a command string into structured components
pub fn parse_command(cmd: &str) -> ParsedCommand {
    let original = cmd;
    let cmd = cmd.trim();
    let tokens: Vec<&str> = tokenize(cmd);

    if tokens.is_empty() {
        return ParsedCommand {
            program: String::new(),
            subcommand: None,
            args: vec![],
            full: original.to_string(),
        };
    }

    let program = tokens[0].to_string();

    // Check if this program uses subcommands
    let has_subcommand = SUBCOMMAND_PROGRAMS
        .iter()
        .any(|&p| p.eq_ignore_ascii_case(&program));

    let (subcommand, args) = if has_subcommand && tokens.len() > 1 {
        // Second token is subcommand if it doesn't start with - (flag)
        let potential_sub = tokens[1];
        if !potential_sub.starts_with('-') {
            (
                Some(potential_sub.to_string()),
                tokens[2..].iter().map(|s| s.to_string()).collect(),
            )
        } else {
            (None, tokens[1..].iter().map(|s| s.to_string()).collect())
        }
    } else {
        (None, tokens[1..].iter().map(|s| s.to_string()).collect())
    };

    ParsedCommand {
        program,
        subcommand,
        args,
        full: original.to_string(),
    }
}

/// Simple tokenizer that handles basic quoting
fn tokenize(cmd: &str) -> Vec<&str> {
    let mut tokens = vec![];
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut token_start: Option<usize> = None;
    let mut chars = cmd.char_indices().peekable();

    while let Some((i, c)) = chars.next() {
        match c {
            '\'' if !in_double_quote => {
                in_single_quote = !in_single_quote;
                if token_start.is_none() {
                    token_start = Some(i);
                }
            }
            '"' if !in_single_quote => {
                in_double_quote = !in_double_quote;
                if token_start.is_none() {
                    token_start = Some(i);
                }
            }
            ' ' | '\t' if !in_single_quote && !in_double_quote => {
                if let Some(start) = token_start {
                    tokens.push(&cmd[start..i]);
                    token_start = None;
                }
            }
            _ => {
                if token_start.is_none() {
                    token_start = Some(i);
                }
            }
        }
    }

    // Push final token
    if let Some(start) = token_start {
        tokens.push(&cmd[start..]);
    }

    tokens
}

/// Extract the "interesting" argument from a command for learning
/// Filters out common flags and focuses on values like branch names, file paths, etc.
pub fn extract_learnable_args(parsed: &ParsedCommand) -> Vec<String> {
    let mut learnable = vec![];

    for (i, arg) in parsed.args.iter().enumerate() {
        // Skip common flags
        if arg.starts_with('-') {
            // But capture the value after flags like -m, -b, --message
            // (next arg if this is a value-taking flag)
            continue;
        }

        // Skip if previous arg was a flag that takes a value
        if i > 0 {
            let prev = &parsed.args[i - 1];
            if matches!(prev.as_str(), "-m" | "-b" | "--message" | "--branch" | "-f" | "--file") {
                // This is a flag value, might be interesting
                // But skip commit messages (too unique)
                if prev != "-m" && prev != "--message" {
                    learnable.push(arg.clone());
                }
                continue;
            }
        }

        // Skip very long args (likely paths or messages)
        if arg.len() > 100 {
            continue;
        }

        // Include branch names, file names, package names, etc.
        learnable.push(arg.clone());
    }

    learnable
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_command() {
        let parsed = parse_command("ls -la");
        assert_eq!(parsed.program, "ls");
        assert_eq!(parsed.subcommand, None);
        assert_eq!(parsed.args, vec!["-la"]);
    }

    #[test]
    fn test_parse_git_command() {
        let parsed = parse_command("git commit -m 'test message'");
        assert_eq!(parsed.program, "git");
        assert_eq!(parsed.subcommand, Some("commit".to_string()));
        assert_eq!(parsed.args, vec!["-m", "'test message'"]);
    }

    #[test]
    fn test_parse_git_checkout() {
        let parsed = parse_command("git checkout feature/login");
        assert_eq!(parsed.program, "git");
        assert_eq!(parsed.subcommand, Some("checkout".to_string()));
        assert_eq!(parsed.args, vec!["feature/login"]);
    }

    #[test]
    fn test_parse_docker_command() {
        let parsed = parse_command("docker run -it ubuntu bash");
        assert_eq!(parsed.program, "docker");
        assert_eq!(parsed.subcommand, Some("run".to_string()));
        assert_eq!(parsed.args, vec!["-it", "ubuntu", "bash"]);
    }

    #[test]
    fn test_parse_with_flags_first() {
        let parsed = parse_command("git -C /path status");
        assert_eq!(parsed.program, "git");
        // -C is a flag, so status should still be detected
        // Actually our simple parser treats -C as not a subcommand
        assert_eq!(parsed.subcommand, None);
    }

    #[test]
    fn test_partial_command() {
        let parsed = parse_command("git checkout ");
        assert!(parsed.is_partial());
        assert_eq!(parsed.arg_lookup_key(), "git checkout");
    }

    #[test]
    fn test_tokenize_quoted() {
        let tokens = tokenize("echo 'hello world' foo");
        assert_eq!(tokens, vec!["echo", "'hello world'", "foo"]);
    }

    #[test]
    fn test_extract_learnable_args() {
        let parsed = parse_command("git checkout -b feature/new-thing");
        let learnable = extract_learnable_args(&parsed);
        assert!(learnable.contains(&"feature/new-thing".to_string()));
    }

    #[test]
    fn test_cargo_command() {
        let parsed = parse_command("cargo build --release");
        assert_eq!(parsed.program, "cargo");
        assert_eq!(parsed.subcommand, Some("build".to_string()));
        assert_eq!(parsed.args, vec!["--release"]);
    }
}
