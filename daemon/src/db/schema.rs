//! Database schema definitions.

/// SQL statements to create the database schema
pub const SCHEMA_V1: &str = r#"
-- Unique commands (deduplicated)
CREATE TABLE IF NOT EXISTS commands (
    id INTEGER PRIMARY KEY,
    argv TEXT NOT NULL UNIQUE
);

-- Directory + host combinations (places)
CREATE TABLE IF NOT EXISTS places (
    id INTEGER PRIMARY KEY,
    host TEXT NOT NULL,
    dir TEXT NOT NULL,
    UNIQUE(host, dir)
);

-- VCS and project context
CREATE TABLE IF NOT EXISTS contexts (
    id INTEGER PRIMARY KEY,
    vcs_type TEXT,           -- 'git', 'hg', or NULL
    vcs_root TEXT,           -- Repository root path
    vcs_branch TEXT,         -- Branch name
    project_type TEXT        -- 'rust', 'node', 'python', etc.
);

-- Sessions (shell instances)
CREATE TABLE IF NOT EXISTS sessions (
    id INTEGER PRIMARY KEY,
    host TEXT NOT NULL,
    pid INTEGER,
    start_time INTEGER NOT NULL,
    end_time INTEGER
);

-- Main history table
CREATE TABLE IF NOT EXISTS history (
    id INTEGER PRIMARY KEY,
    session_id INTEGER,      -- Shell session (not FK for now, sessions managed separately)
    command_id INTEGER NOT NULL REFERENCES commands(id),
    place_id INTEGER NOT NULL REFERENCES places(id),
    context_id INTEGER REFERENCES contexts(id),
    start_time INTEGER NOT NULL,
    duration REAL,           -- Duration in seconds (float for sub-second precision)
    exit_status INTEGER,
    time_bucket INTEGER      -- Hour of day (0-23) for time-of-day patterns
);

-- N-gram tables for prediction

-- Bigram: P(command | prev_command)
CREATE TABLE IF NOT EXISTS ngrams_2 (
    prev_command_id INTEGER NOT NULL REFERENCES commands(id),
    command_id INTEGER NOT NULL REFERENCES commands(id),
    frequency INTEGER NOT NULL DEFAULT 1,
    last_used INTEGER NOT NULL,
    PRIMARY KEY (prev_command_id, command_id)
);

-- Trigram: P(command | prev2_command, prev1_command)
CREATE TABLE IF NOT EXISTS ngrams_3 (
    prev2_command_id INTEGER NOT NULL REFERENCES commands(id),
    prev1_command_id INTEGER NOT NULL REFERENCES commands(id),
    command_id INTEGER NOT NULL REFERENCES commands(id),
    frequency INTEGER NOT NULL DEFAULT 1,
    last_used INTEGER NOT NULL,
    PRIMARY KEY (prev2_command_id, prev1_command_id, command_id)
);

-- Directory-command frequency (for context scoring)
CREATE TABLE IF NOT EXISTS dir_command_freq (
    place_id INTEGER NOT NULL REFERENCES places(id),
    command_id INTEGER NOT NULL REFERENCES commands(id),
    frequency INTEGER NOT NULL DEFAULT 1,
    last_used INTEGER NOT NULL,
    PRIMARY KEY (place_id, command_id)
);

-- Parsed commands for argument-aware suggestions
-- e.g., "git commit -m 'fix'" -> program='git', subcommand='commit', args='-m fix'
CREATE TABLE IF NOT EXISTS parsed_commands (
    command_id INTEGER PRIMARY KEY REFERENCES commands(id),
    program TEXT NOT NULL,           -- First token (git, docker, npm, etc.)
    subcommand TEXT,                 -- Second token if common pattern (commit, push, run, etc.)
    args_hash TEXT                   -- Hash of remaining args for dedup
);

-- Argument patterns: what arguments follow a given program+subcommand
-- e.g., after "git checkout" -> "main", "develop", "feature/login"
CREATE TABLE IF NOT EXISTS arg_patterns (
    id INTEGER PRIMARY KEY,
    program TEXT NOT NULL,
    subcommand TEXT,                 -- NULL for single-word commands
    arg_value TEXT NOT NULL,         -- The argument seen
    frequency INTEGER NOT NULL DEFAULT 1,
    last_used INTEGER NOT NULL,
    place_id INTEGER REFERENCES places(id),  -- Optional: dir-specific args
    UNIQUE(program, subcommand, arg_value, place_id)
);

-- Frecent paths (fasd-like frecency tracking)
CREATE TABLE IF NOT EXISTS frecent_paths (
    id INTEGER PRIMARY KEY,
    path TEXT NOT NULL,
    path_type TEXT NOT NULL DEFAULT 'd',  -- 'd' = directory, 'f' = file
    rank REAL NOT NULL DEFAULT 1.0,
    last_access INTEGER NOT NULL,
    access_count INTEGER NOT NULL DEFAULT 1,
    UNIQUE(path, path_type)
);

-- Schema version tracking
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    applied_at INTEGER NOT NULL
);

-- Indexes for fast queries
CREATE INDEX IF NOT EXISTS idx_history_command_id ON history(command_id);
CREATE INDEX IF NOT EXISTS idx_history_place_id ON history(place_id);
CREATE INDEX IF NOT EXISTS idx_history_start_time ON history(start_time DESC);
CREATE INDEX IF NOT EXISTS idx_history_time_bucket ON history(time_bucket);
CREATE INDEX IF NOT EXISTS idx_commands_argv ON commands(argv);
CREATE INDEX IF NOT EXISTS idx_places_dir ON places(dir);
CREATE INDEX IF NOT EXISTS idx_ngrams_2_prev ON ngrams_2(prev_command_id);
CREATE INDEX IF NOT EXISTS idx_ngrams_3_prev ON ngrams_3(prev2_command_id, prev1_command_id);
CREATE INDEX IF NOT EXISTS idx_parsed_commands_program ON parsed_commands(program);
CREATE INDEX IF NOT EXISTS idx_parsed_commands_subcommand ON parsed_commands(program, subcommand);
CREATE INDEX IF NOT EXISTS idx_arg_patterns_lookup ON arg_patterns(program, subcommand);
CREATE INDEX IF NOT EXISTS idx_arg_patterns_place ON arg_patterns(place_id);
CREATE INDEX IF NOT EXISTS idx_frecent_paths_type ON frecent_paths(path_type);
CREATE INDEX IF NOT EXISTS idx_frecent_paths_rank ON frecent_paths(rank DESC);
CREATE INDEX IF NOT EXISTS idx_frecent_paths_path ON frecent_paths(path);
"#;

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use super::*;

    #[test]
    fn test_schema_valid_sql() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA_V1).unwrap();

        // Verify tables exist
        let tables: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();

        assert!(tables.contains(&"commands".to_string()));
        assert!(tables.contains(&"places".to_string()));
        assert!(tables.contains(&"contexts".to_string()));
        assert!(tables.contains(&"history".to_string()));
        assert!(tables.contains(&"ngrams_2".to_string()));
        assert!(tables.contains(&"ngrams_3".to_string()));
        assert!(tables.contains(&"frecent_paths".to_string()));
    }

    #[test]
    fn test_schema_indexes() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA_V1).unwrap();

        // Verify indexes exist
        let indexes: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();

        assert!(indexes.contains(&"idx_history_command_id".to_string()));
        assert!(indexes.contains(&"idx_history_start_time".to_string()));
        assert!(indexes.contains(&"idx_commands_argv".to_string()));
    }
}
