//! SQLite database layer for nicehist.

mod migrations;
mod schema;

use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use rusqlite::Connection;
use tracing::debug;

use crate::prediction::parser::{extract_learnable_args, parse_command};
use crate::protocol::{
    ContextInfo, FrecentAddParams, FrecentQueryParams, FrecencyResult, PredictParams,
    SearchParams, SearchResult, StoreParams, Suggestion,
};

/// Thread-safe database handle
#[derive(Clone)]
pub struct Database {
    conn: Arc<Mutex<Connection>>,
}

impl Database {
    /// Open or create a database at the given path
    pub fn open(path: &Path) -> Result<Self> {
        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create directory: {}", parent.display()))?;
        }

        let conn = Connection::open(path)
            .with_context(|| format!("Failed to open database: {}", path.display()))?;

        // Enable WAL mode for concurrent access
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;

        let db = Self {
            conn: Arc::new(Mutex::new(conn)),
        };

        // Run migrations
        db.migrate()?;

        Ok(db)
    }

    /// Open an in-memory database (for testing)
    #[allow(dead_code)]
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.pragma_update(None, "foreign_keys", "ON")?;

        let db = Self {
            conn: Arc::new(Mutex::new(conn)),
        };

        db.migrate()?;

        Ok(db)
    }

    /// Run database migrations
    fn migrate(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        migrations::run_migrations(&conn)
    }

    /// Store a command in the database
    pub fn store_command(&self, params: &StoreParams) -> Result<i64> {
        let conn = self.conn.lock().unwrap();

        // Get or create command ID
        let command_id = self.get_or_create_command(&conn, &params.cmd)?;

        // Get or create place ID
        let hostname = hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| "unknown".to_string());
        let place_id = self.get_or_create_place(&conn, &hostname, &params.cwd)?;

        // Get or detect context
        let context_id = self.get_or_create_context_for_dir(&conn, &params.cwd)?;

        // Calculate time bucket (hour of day)
        let start_time = params
            .start_time
            .unwrap_or_else(|| chrono_lite_timestamp());
        let time_bucket = ((start_time % 86400) / 3600) as i32;

        // Insert history entry
        conn.execute(
            "INSERT INTO history (session_id, command_id, place_id, context_id, start_time, duration, exit_status, time_bucket)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            rusqlite::params![
                params.session_id,
                command_id,
                place_id,
                context_id,
                start_time,
                params.duration_ms.map(|d| d as f64 / 1000.0),
                params.exit_status,
                time_bucket,
            ],
        )?;

        let history_id = conn.last_insert_rowid();

        // Update n-grams if previous command provided
        if let Some(ref prev_cmd) = params.prev_cmd {
            let prev_id = self.get_or_create_command(&conn, prev_cmd)?;
            self.update_bigram(&conn, prev_id, command_id)?;

            if let Some(ref prev2_cmd) = params.prev2_cmd {
                let prev2_id = self.get_or_create_command(&conn, prev2_cmd)?;
                self.update_trigram(&conn, prev2_id, prev_id, command_id)?;
            }
        }

        // Store parsed command for argument suggestions
        self.store_parsed_command(&conn, command_id, &params.cmd)?;

        // Store argument patterns
        self.store_arg_patterns(&conn, &params.cmd, Some(place_id))?;

        // Extract frecent paths from command arguments
        self.extract_frecent_paths(&conn, &params.cmd, &params.cwd)?;

        debug!("Stored command {} with history_id {}", params.cmd, history_id);
        Ok(history_id)
    }

    fn get_or_create_command(&self, conn: &Connection, argv: &str) -> Result<i64> {
        // Try to find existing
        let mut stmt = conn.prepare_cached("SELECT id FROM commands WHERE argv = ?1")?;
        let result: Option<i64> = stmt.query_row([argv], |row| row.get(0)).ok();

        if let Some(id) = result {
            return Ok(id);
        }

        // Create new
        conn.execute("INSERT INTO commands (argv) VALUES (?1)", [argv])?;
        Ok(conn.last_insert_rowid())
    }

    fn get_or_create_place(&self, conn: &Connection, host: &str, dir: &str) -> Result<i64> {
        let mut stmt =
            conn.prepare_cached("SELECT id FROM places WHERE host = ?1 AND dir = ?2")?;
        let result: Option<i64> = stmt.query_row([host, dir], |row| row.get(0)).ok();

        if let Some(id) = result {
            return Ok(id);
        }

        conn.execute(
            "INSERT INTO places (host, dir) VALUES (?1, ?2)",
            [host, dir],
        )?;
        Ok(conn.last_insert_rowid())
    }

    fn get_or_create_context_for_dir(&self, _conn: &Connection, _dir: &str) -> Result<Option<i64>> {
        // For now, return None - context detection will be implemented later
        // This will be filled in by the context module
        Ok(None)
    }

    fn update_bigram(&self, conn: &Connection, prev_id: i64, cmd_id: i64) -> Result<()> {
        let now = chrono_lite_timestamp();
        conn.execute(
            "INSERT INTO ngrams_2 (prev_command_id, command_id, frequency, last_used)
             VALUES (?1, ?2, 1, ?3)
             ON CONFLICT(prev_command_id, command_id) DO UPDATE SET
                frequency = frequency + 1,
                last_used = ?3",
            rusqlite::params![prev_id, cmd_id, now],
        )?;
        Ok(())
    }

    fn update_trigram(
        &self,
        conn: &Connection,
        prev2_id: i64,
        prev1_id: i64,
        cmd_id: i64,
    ) -> Result<()> {
        let now = chrono_lite_timestamp();
        conn.execute(
            "INSERT INTO ngrams_3 (prev2_command_id, prev1_command_id, command_id, frequency, last_used)
             VALUES (?1, ?2, ?3, 1, ?4)
             ON CONFLICT(prev2_command_id, prev1_command_id, command_id) DO UPDATE SET
                frequency = frequency + 1,
                last_used = ?4",
            rusqlite::params![prev2_id, prev1_id, cmd_id, now],
        )?;
        Ok(())
    }

    /// Store parsed command for argument-aware suggestions
    fn store_parsed_command(&self, conn: &Connection, command_id: i64, cmd: &str) -> Result<()> {
        let parsed = parse_command(cmd);

        if parsed.program.is_empty() {
            return Ok(());
        }

        // Store parsed command
        conn.execute(
            "INSERT OR REPLACE INTO parsed_commands (command_id, program, subcommand, args_hash)
             VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![
                command_id,
                &parsed.program,
                &parsed.subcommand,
                parsed.args.join(" ").get(..50), // Truncate args hash
            ],
        )?;

        Ok(())
    }

    /// Store argument patterns for argument-aware suggestions
    fn store_arg_patterns(
        &self,
        conn: &Connection,
        cmd: &str,
        place_id: Option<i64>,
    ) -> Result<()> {
        let parsed = parse_command(cmd);
        let learnable = extract_learnable_args(&parsed);
        let now = chrono_lite_timestamp();

        for arg in learnable {
            // Skip very short or very long args
            if arg.len() < 2 || arg.len() > 100 {
                continue;
            }

            conn.execute(
                "INSERT INTO arg_patterns (program, subcommand, arg_value, frequency, last_used, place_id)
                 VALUES (?1, ?2, ?3, 1, ?4, ?5)
                 ON CONFLICT(program, subcommand, arg_value, place_id) DO UPDATE SET
                    frequency = frequency + 1,
                    last_used = ?4",
                rusqlite::params![
                    &parsed.program,
                    &parsed.subcommand,
                    &arg,
                    now,
                    place_id,
                ],
            )?;
        }

        Ok(())
    }

    /// Get argument suggestions for a partial command
    pub fn get_arg_suggestions(
        &self,
        prefix: &str,
        cwd: &str,
        limit: usize,
    ) -> Result<Vec<Suggestion>> {
        let conn = self.conn.lock().unwrap();
        let parsed = parse_command(prefix);

        // Only suggest args if command ends with space (expecting argument)
        if !parsed.is_partial() {
            return Ok(vec![]);
        }

        let hostname = hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| "unknown".to_string());

        // Get place_id for directory-specific args
        let place_id: Option<i64> = conn
            .query_row(
                "SELECT id FROM places WHERE host = ?1 AND dir = ?2",
                [&hostname, cwd],
                |row| row.get(0),
            )
            .ok();

        // Query for argument patterns
        let mut stmt = conn.prepare_cached(
            "SELECT arg_value, SUM(frequency) as total_freq,
                    SUM(CASE WHEN place_id = ?4 THEN frequency ELSE 0 END) as dir_freq
             FROM arg_patterns
             WHERE program = ?1 AND (subcommand = ?2 OR (subcommand IS NULL AND ?2 IS NULL))
             GROUP BY arg_value
             ORDER BY dir_freq DESC, total_freq DESC
             LIMIT ?3",
        )?;

        let rows = stmt.query_map(
            rusqlite::params![&parsed.program, &parsed.subcommand, limit, place_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )?;

        let mut suggestions = Vec::new();
        for row in rows {
            if let Ok((arg, total_freq, dir_freq)) = row {
                let base_score = (total_freq as f64).ln().max(0.0) / 10.0;
                let dir_bonus = if dir_freq > 0 { 0.3 } else { 0.0 };
                let score = (base_score + dir_bonus).min(1.0);

                // Build full command with this argument
                let full_cmd = format!("{}{}", prefix, arg);
                suggestions.push(Suggestion {
                    cmd: full_cmd,
                    score,
                });
            }
        }

        Ok(suggestions)
    }

    /// Get predictions based on prefix and context
    pub fn predict(&self, params: &PredictParams) -> Result<Vec<Suggestion>> {
        let conn = self.conn.lock().unwrap();

        // Get hostname for place matching
        let hostname = hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| "unknown".to_string());

        // Check if this is a partial command expecting arguments
        let parsed = parse_command(&params.prefix);
        let expecting_args = parsed.is_partial() && !parsed.program.is_empty();

        // Strategy 0: Argument suggestions if expecting args
        if expecting_args {
            drop(conn); // Release lock for get_arg_suggestions
            let arg_suggestions = self.get_arg_suggestions(&params.prefix, &params.cwd, params.limit)?;
            if !arg_suggestions.is_empty() {
                return Ok(arg_suggestions);
            }
            // Re-acquire lock if no arg suggestions
            let conn = self.conn.lock().unwrap();

            // Continue with regular predictions below using this conn
            return self.predict_with_conn(&conn, params, &hostname);
        }

        self.predict_with_conn(&conn, params, &hostname)
    }

    fn predict_with_conn(
        &self,
        conn: &Connection,
        params: &PredictParams,
        hostname: &str,
    ) -> Result<Vec<Suggestion>> {
        let mut suggestions = Vec::new();

        // Strategy 1: Compute n-gram bonus scores (additive, applied in strategy 2)
        // Trigrams (prev2 → prev1 → ?) are a stronger signal than bigrams (prev1 → ?)
        let mut ngram_bonus: std::collections::HashMap<String, f64> = std::collections::HashMap::new();
        if !params.last_cmds.is_empty() {
            let prev1_cmd = &params.last_cmds[0];
            if let Ok(prev1_id) = self.get_command_id(conn, prev1_cmd) {
                // Trigram lookup: if we have two previous commands
                if params.last_cmds.len() >= 2 {
                    let prev2_cmd = &params.last_cmds[1];
                    if let Ok(prev2_id) = self.get_command_id(conn, prev2_cmd) {
                        let mut stmt = conn.prepare_cached(
                            "SELECT c.argv, n.frequency
                             FROM ngrams_3 n
                             JOIN commands c ON c.id = n.command_id
                             WHERE n.prev2_command_id = ?1 AND n.prev1_command_id = ?2
                               AND c.argv LIKE ?3 || '%'
                             ORDER BY n.frequency DESC
                             LIMIT ?4",
                        )?;

                        let rows = stmt.query_map(
                            rusqlite::params![prev2_id, prev1_id, params.prefix, params.limit],
                            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
                        )?;

                        for row in rows {
                            if let Ok((cmd, freq)) = row {
                                // Trigrams get a 1.5x multiplier over bigrams (stronger signal)
                                let bonus = ((freq as f64).ln().max(0.0) / 10.0) * 1.5;
                                ngram_bonus.insert(cmd, bonus.min(1.0));
                            }
                        }
                    }
                }

                // Bigram lookup: prev1 → ?
                let mut stmt = conn.prepare_cached(
                    "SELECT c.argv, n.frequency
                     FROM ngrams_2 n
                     JOIN commands c ON c.id = n.command_id
                     WHERE n.prev_command_id = ?1 AND c.argv LIKE ?2 || '%'
                     ORDER BY n.frequency DESC
                     LIMIT ?3",
                )?;

                let rows = stmt.query_map(
                    rusqlite::params![prev1_id, params.prefix, params.limit],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
                )?;

                for row in rows {
                    if let Ok((cmd, freq)) = row {
                        let bonus = (freq as f64).ln().max(0.0) / 10.0;
                        // Only insert if trigram didn't already provide a higher bonus
                        ngram_bonus.entry(cmd).or_insert(bonus.min(1.0));
                    }
                }
            }
        }

        // Strategy 2: Prefix match with recency, directory, and parent directory weighting
        // Build list of directories to check (current + ancestors)
        let dir_list = get_directory_hierarchy(&params.cwd, 3);
        let dir_placeholders: Vec<String> = dir_list.iter().enumerate().map(|(i, _)| format!("?{}", i + 5)).collect();
        let dir_case = if !dir_placeholders.is_empty() {
            format!(
                "SUM(CASE WHEN p.dir IN ({}) THEN 1.0 / (1 + (LENGTH(?2) - LENGTH(p.dir)) / 10.0) ELSE 0 END)",
                dir_placeholders.join(", ")
            )
        } else {
            "0".to_string()
        };

        let query = format!(
            "SELECT c.argv, COUNT(*) as freq, MAX(h.start_time) as last_used,
                    SUM(CASE WHEN p.dir = ?2 THEN 1 ELSE 0 END) as exact_dir_freq,
                    {} as hierarchy_score,
                    CAST(SUM(CASE WHEN h.exit_status != 0 AND h.exit_status IS NOT NULL THEN 1 ELSE 0 END) AS REAL) / COUNT(*) as failure_rate
             FROM history h
             JOIN commands c ON c.id = h.command_id
             JOIN places p ON p.id = h.place_id
             WHERE c.argv LIKE ?1 || '%' AND p.host = ?3
             GROUP BY c.id
             ORDER BY exact_dir_freq DESC, hierarchy_score DESC, last_used DESC
             LIMIT ?4",
            dir_case
        );

        let mut stmt = conn.prepare(&query)?;

        // Build params array
        let mut query_params: Vec<Box<dyn rusqlite::ToSql>> = vec![
            Box::new(params.prefix.clone()),
            Box::new(params.cwd.clone()),
            Box::new(hostname.to_string()),
            Box::new(params.limit * 2),
        ];
        for dir in &dir_list {
            query_params.push(Box::new(dir.clone()));
        }

        let params_refs: Vec<&dyn rusqlite::ToSql> = query_params.iter().map(|p| p.as_ref()).collect();

        let rows = stmt.query_map(params_refs.as_slice(), |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, f64>(4).unwrap_or(0.0),
                row.get::<_, f64>(5).unwrap_or(0.0),
            ))
        })?;

        let now = chrono_lite_timestamp();
        let w = params.weights.clone().unwrap_or_default();

        // Cross-pollination: boost predictions in frecent directories
        let frecent_boost = if params.frecent_boost {
            let frecent_rank: f64 = conn
                .query_row(
                    "SELECT rank FROM frecent_paths WHERE path = ?1 AND path_type = 'd'",
                    [&params.cwd],
                    |row| row.get(0),
                )
                .unwrap_or(0.0);
            // Normalize: log(rank+1) / 100, capped at configured max
            (frecent_rank.ln_1p() / 100.0).min(w.frecent_boost_max)
        } else {
            0.0
        };

        for row in rows {
            if let Ok((cmd, freq, last_used, exact_dir_freq, hierarchy_score, failure_rate)) = row {
                // Calculate score based on frequency, recency, and directory match
                let age_days = (now - last_used) as f64 / 86400.0;
                let recency_score = (-age_days / 30.0).exp(); // Decay over 30 days
                let freq_score = (freq as f64).ln().max(0.0) / 10.0;

                // Directory scoring: exact match > parent match
                let dir_score = if exact_dir_freq > 0 {
                    w.dir_exact
                } else if hierarchy_score > 0.0 {
                    w.dir_hierarchy * hierarchy_score.min(1.0)
                } else {
                    0.0
                };

                // N-gram bonus: commands that follow the previous command get a boost
                let ngram_score = ngram_bonus.get(&cmd).copied().unwrap_or(0.0) * w.ngram;

                // Penalize commands that frequently fail
                let failure_penalty = 1.0 - (failure_rate * w.failure_penalty);
                let score = (freq_score * w.frequency + recency_score * w.recency + dir_score + frecent_boost + ngram_score).min(1.0) * failure_penalty;

                suggestions.push(Suggestion { cmd, score });
            }
        }

        // Sort by score and limit
        suggestions.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
        suggestions.truncate(params.limit);

        Ok(suggestions)
    }

    fn get_command_id(&self, conn: &Connection, argv: &str) -> Result<i64> {
        let mut stmt = conn.prepare_cached("SELECT id FROM commands WHERE argv = ?1")?;
        let id: i64 = stmt.query_row([argv], |row| row.get(0))?;
        Ok(id)
    }

    /// Add or bump a path's frecency (fasd-like ranking)
    pub fn frecent_add(&self, params: &FrecentAddParams) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        self.frecent_add_with_conn(&conn, &params.path, &params.path_type, params.rank, params.timestamp)
    }

    fn frecent_add_with_conn(
        &self,
        conn: &Connection,
        path: &str,
        path_type: &str,
        rank_override: Option<f64>,
        timestamp_override: Option<i64>,
    ) -> Result<()> {
        let now = timestamp_override.unwrap_or_else(chrono_lite_timestamp);

        if let Some(rank) = rank_override {
            // Import mode: use provided rank/timestamp directly
            conn.execute(
                "INSERT INTO frecent_paths (path, path_type, rank, last_access, access_count)
                 VALUES (?1, ?2, ?3, ?4, 1)
                 ON CONFLICT(path, path_type) DO UPDATE SET
                    rank = MAX(rank, ?3),
                    last_access = MAX(last_access, ?4),
                    access_count = access_count + 1",
                rusqlite::params![path, path_type, rank, now],
            )?;
        } else {
            // Normal mode: fasd rank formula
            // new_rank = old_rank + 1/old_rank (or 1.0 for new entries)
            conn.execute(
                "INSERT INTO frecent_paths (path, path_type, rank, last_access, access_count)
                 VALUES (?1, ?2, 1.0, ?3, 1)
                 ON CONFLICT(path, path_type) DO UPDATE SET
                    rank = rank + 1.0 / MAX(rank, 0.01),
                    last_access = ?3,
                    access_count = access_count + 1",
                rusqlite::params![path, path_type, now],
            )?;
        }

        // Aging: if total rank for this path_type exceeds 2000, decay all by 0.9
        let total_rank: f64 = conn
            .query_row(
                "SELECT COALESCE(SUM(rank), 0.0) FROM frecent_paths WHERE path_type = ?1",
                [path_type],
                |row| row.get(0),
            )
            .unwrap_or(0.0);

        if total_rank > 2000.0 {
            conn.execute(
                "UPDATE frecent_paths SET rank = rank * 0.9 WHERE path_type = ?1",
                [path_type],
            )?;
            // Prune entries with rank < 1.0
            conn.execute(
                "DELETE FROM frecent_paths WHERE path_type = ?1 AND rank < 1.0",
                [path_type],
            )?;
        }

        Ok(())
    }

    /// Query frecent paths with fasd-compatible matching and scoring
    pub fn frecent_query(&self, params: &FrecentQueryParams) -> Result<Vec<FrecencyResult>> {
        let conn = self.conn.lock().unwrap();
        let now = chrono_lite_timestamp();

        // Fetch all candidate paths (filtered by type)
        let query = if let Some(ref pt) = params.path_type {
            format!(
                "SELECT path, path_type, rank, last_access FROM frecent_paths WHERE path_type = '{}' ORDER BY rank DESC",
                if pt == "f" { "f" } else { "d" }
            )
        } else {
            "SELECT path, path_type, rank, last_access FROM frecent_paths ORDER BY rank DESC".to_string()
        };

        let mut stmt = conn.prepare(&query)?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, f64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })?;

        let mut candidates: Vec<(String, String, f64, i64)> = Vec::new();
        for row in rows {
            if let Ok(r) = row {
                candidates.push(r);
            }
        }

        let raw = params.raw;

        // If no search terms, return all by frecency score
        if params.terms.is_empty() {
            let mut results: Vec<FrecencyResult> = candidates
                .iter()
                .map(|(path, path_type, rank, last_access)| FrecencyResult {
                    path: path.clone(),
                    path_type: path_type.clone(),
                    score: frecency_score(*rank, *last_access, now),
                    rank: if raw { Some(*rank) } else { None },
                    last_access: if raw { Some(*last_access) } else { None },
                })
                .collect();
            results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
            results.truncate(params.limit);
            return Ok(results);
        }

        // Three-tier matching
        let mut results: Vec<FrecencyResult> = Vec::new();

        // Tier 1: Ordered substring match (case-sensitive)
        for (path, path_type, rank, last_access) in &candidates {
            if matches_ordered_substring(path, &params.terms, false) {
                results.push(FrecencyResult {
                    path: path.clone(),
                    path_type: path_type.clone(),
                    score: frecency_score(*rank, *last_access, now),
                    rank: if raw { Some(*rank) } else { None },
                    last_access: if raw { Some(*last_access) } else { None },
                });
            }
        }

        // Tier 2: Case-insensitive ordered substring
        if results.is_empty() {
            for (path, path_type, rank, last_access) in &candidates {
                if matches_ordered_substring(path, &params.terms, true) {
                    results.push(FrecencyResult {
                        path: path.clone(),
                        path_type: path_type.clone(),
                        score: frecency_score(*rank, *last_access, now),
                        rank: if raw { Some(*rank) } else { None },
                        last_access: if raw { Some(*last_access) } else { None },
                    });
                }
            }
        }

        // Tier 3: Fuzzy match (each char of each term in order)
        if results.is_empty() {
            for (path, path_type, rank, last_access) in &candidates {
                if matches_fuzzy(path, &params.terms) {
                    results.push(FrecencyResult {
                        path: path.clone(),
                        path_type: path_type.clone(),
                        score: frecency_score(*rank, *last_access, now),
                        rank: if raw { Some(*rank) } else { None },
                        last_access: if raw { Some(*last_access) } else { None },
                    });
                }
            }
        }

        results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(params.limit);
        Ok(results)
    }

    /// Extract frecent paths from a command's arguments and bump their frecency
    fn extract_frecent_paths(&self, conn: &Connection, cmd: &str, cwd: &str) -> Result<()> {
        use std::path::PathBuf;

        // Always bump the cwd as a directory
        self.frecent_add_with_conn(conn, cwd, "d", None, None)?;

        let parsed = parse_command(cmd);
        let mut count = 0;

        for arg in &parsed.args {
            if count >= 5 {
                break;
            }

            // Skip flags
            if arg.starts_with('-') {
                continue;
            }

            // Skip args that don't look like paths
            if !arg.contains('/') && !arg.contains('.') && arg.len() > 50 {
                continue;
            }

            // Resolve relative paths against cwd
            let path = if arg.starts_with('/') || arg.starts_with('~') {
                let expanded = if arg.starts_with('~') {
                    if let Ok(home) = std::env::var("HOME") {
                        arg.replacen('~', &home, 1)
                    } else {
                        continue;
                    }
                } else {
                    arg.to_string()
                };
                PathBuf::from(expanded)
            } else {
                PathBuf::from(cwd).join(arg)
            };

            // Check if path exists and categorize
            if let Ok(meta) = std::fs::metadata(&path) {
                let path_str = path.to_string_lossy().to_string();
                if meta.is_dir() {
                    self.frecent_add_with_conn(conn, &path_str, "d", None, None)?;
                } else if meta.is_file() {
                    self.frecent_add_with_conn(conn, &path_str, "f", None, None)?;
                }
                count += 1;
            }
        }

        Ok(())
    }

    /// Get context information for a directory
    #[allow(dead_code)]
    pub fn get_context(&self, _cwd: &str) -> Result<ContextInfo> {
        // For now, return empty context - will be filled by context module
        Ok(ContextInfo {
            vcs: None,
            branch: None,
            vcs_root: None,
            project: None,
        })
    }

    /// Delete a command and all its references from the database
    pub fn delete_command(&self, cmd: &str) -> Result<u64> {
        let conn = self.conn.lock().unwrap();

        // Look up command_id
        let command_id: i64 = conn
            .query_row("SELECT id FROM commands WHERE argv = ?1", [cmd], |row| {
                row.get(0)
            })
            .with_context(|| format!("Command not found: {}", cmd))?;

        // Delete from all referencing tables
        conn.execute("DELETE FROM history WHERE command_id = ?1", [command_id])?;
        conn.execute(
            "DELETE FROM ngrams_2 WHERE command_id = ?1 OR prev_command_id = ?1",
            [command_id],
        )?;
        conn.execute(
            "DELETE FROM ngrams_3 WHERE command_id = ?1 OR prev1_command_id = ?1 OR prev2_command_id = ?1",
            [command_id],
        )?;
        conn.execute(
            "DELETE FROM dir_command_freq WHERE command_id = ?1",
            [command_id],
        )?;
        conn.execute(
            "DELETE FROM parsed_commands WHERE command_id = ?1",
            [command_id],
        )?;

        // Clean up arg_patterns from this command's program/subcommand
        let parsed = parse_command(cmd);
        if !parsed.program.is_empty() {
            if let Some(ref sub) = parsed.subcommand {
                conn.execute(
                    "DELETE FROM arg_patterns WHERE program = ?1 AND subcommand = ?2",
                    rusqlite::params![parsed.program, sub],
                )?;
            }
        }

        // Delete the command itself
        let deleted = conn.execute("DELETE FROM commands WHERE id = ?1", [command_id])?;

        Ok(deleted as u64)
    }

    /// Search history
    pub fn search(&self, params: &SearchParams) -> Result<Vec<SearchResult>> {
        let conn = self.conn.lock().unwrap();

        let hostname = hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| "unknown".to_string());

        let query = if params.dir.is_some() {
            "SELECT c.argv, p.dir, MAX(h.start_time) as last_used,
                    h.exit_status, h.duration,
                    COUNT(*) as cmd_freq,
                    CAST(SUM(CASE WHEN h.exit_status != 0 AND h.exit_status IS NOT NULL THEN 1 ELSE 0 END) AS REAL)
                        / COUNT(*) as failure_rate
             FROM history h
             JOIN commands c ON c.id = h.command_id
             JOIN places p ON p.id = h.place_id
             WHERE c.argv LIKE '%' || ?1 || '%'
               AND p.host = ?2
               AND p.dir = ?3
             GROUP BY c.id
             ORDER BY last_used DESC
             LIMIT ?4"
        } else {
            "SELECT c.argv, p.dir, MAX(h.start_time) as last_used,
                    h.exit_status, h.duration,
                    COUNT(*) as cmd_freq,
                    CAST(SUM(CASE WHEN h.exit_status != 0 AND h.exit_status IS NOT NULL THEN 1 ELSE 0 END) AS REAL)
                        / COUNT(*) as failure_rate
             FROM history h
             JOIN commands c ON c.id = h.command_id
             JOIN places p ON p.id = h.place_id
             WHERE c.argv LIKE '%' || ?1 || '%'
               AND p.host = ?2
             GROUP BY c.id
             ORDER BY last_used DESC
             LIMIT ?3"
        };

        let now = chrono_lite_timestamp();
        let mut stmt = conn.prepare(query)?;

        let map_row = |row: &rusqlite::Row| {
            let timestamp: i64 = row.get(2)?;
            let exit_status: Option<i32> = row.get(3)?;
            let cmd_freq: i64 = row.get(5)?;
            let failure_rate: f64 = row.get::<_, f64>(6).unwrap_or(0.0);

            let age_days = (now - timestamp) as f64 / 86400.0;
            let recency_score = (-age_days / 30.0_f64).exp();
            let freq_score = (cmd_freq as f64).ln().max(0.0) / 10.0;
            let failure_penalty = 1.0 - (failure_rate * 0.5);
            let score = (freq_score * 0.35 + recency_score * 0.30).min(1.0) * failure_penalty;

            Ok(SearchResult {
                cmd: row.get(0)?,
                cwd: row.get(1)?,
                timestamp,
                exit_status,
                duration_ms: row.get::<_, Option<f64>>(4)?.map(|d| (d * 1000.0) as i64),
                score: Some(score),
            })
        };

        let mut results: Vec<SearchResult> = if let Some(ref dir) = params.dir {
            stmt.query_map(
                rusqlite::params![params.pattern, hostname, dir, params.limit],
                map_row,
            )?
            .filter_map(|r| r.ok())
            .collect()
        } else {
            stmt.query_map(
                rusqlite::params![params.pattern, hostname, params.limit],
                map_row,
            )?
            .filter_map(|r| r.ok())
            .collect()
        };

        // Sort by score descending (highest relevance first)
        results.sort_by(|a, b| {
            b.score.unwrap_or(0.0).partial_cmp(&a.score.unwrap_or(0.0))
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        Ok(results)
    }
}

/// Calculate frecency score using fasd's time-weighted formula
fn frecency_score(rank: f64, last_access: i64, now: i64) -> f64 {
    let age = (now - last_access).max(0) as f64;
    let weight = if age < 3600.0 {
        6.0 // Within 1 hour
    } else if age < 86400.0 {
        4.0 // Within 1 day
    } else if age < 604800.0 {
        2.0 // Within 1 week
    } else {
        1.0 // Older
    };
    rank * weight
}

/// Check if all terms match as ordered substrings in the path
fn matches_ordered_substring(path: &str, terms: &[String], case_insensitive: bool) -> bool {
    let haystack = if case_insensitive {
        path.to_lowercase()
    } else {
        path.to_string()
    };

    let mut search_from = 0;
    for term in terms {
        let needle = if case_insensitive {
            term.to_lowercase()
        } else {
            term.to_string()
        };
        if let Some(pos) = haystack[search_from..].find(&needle) {
            search_from += pos + needle.len();
        } else {
            return false;
        }
    }
    true
}

/// Fuzzy match: each character of each term appears in order in the path
fn matches_fuzzy(path: &str, terms: &[String]) -> bool {
    let path_lower = path.to_lowercase();
    let mut path_chars = path_lower.chars().peekable();

    for term in terms {
        let term_lower = term.to_lowercase();
        for tc in term_lower.chars() {
            loop {
                match path_chars.next() {
                    Some(pc) if pc == tc => break,
                    Some(_) => continue,
                    None => return false,
                }
            }
        }
    }
    true
}

/// Get current Unix timestamp (simple implementation without chrono dependency)
fn chrono_lite_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Get directory and its ancestors up to max_depth
fn get_directory_hierarchy(dir: &str, max_depth: usize) -> Vec<String> {
    use std::path::Path;

    let mut dirs = vec![dir.to_string()];
    let mut current = Path::new(dir);

    for _ in 0..max_depth {
        if let Some(parent) = current.parent() {
            let parent_str = parent.to_string_lossy().to_string();
            if parent_str.is_empty() || parent_str == "/" {
                break;
            }
            dirs.push(parent_str);
            current = parent;
        } else {
            break;
        }
    }

    dirs
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_open_in_memory() {
        let db = Database::open_in_memory().unwrap();
        assert!(db.conn.lock().is_ok());
    }

    #[test]
    fn test_store_and_retrieve_command() {
        let db = Database::open_in_memory().unwrap();

        let params = StoreParams {
            cmd: "git status".to_string(),
            cwd: "/home/user/project".to_string(),
            exit_status: Some(0),
            duration_ms: Some(100),
            start_time: Some(1700000000),
            session_id: Some(12345),
            prev_cmd: None,
            prev2_cmd: None,
        };

        let id = db.store_command(&params).unwrap();
        assert!(id > 0);

        // Search for the command
        let search_params = SearchParams {
            pattern: "git".to_string(),
            limit: 10,
            dir: None,
            exit_status: None,
        };

        let results = db.search(&search_params).unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].cmd, "git status");
    }

    #[test]
    fn test_ngram_updates() {
        let db = Database::open_in_memory().unwrap();

        // Store first command
        let params1 = StoreParams {
            cmd: "git add -A".to_string(),
            cwd: "/home/user/project".to_string(),
            exit_status: Some(0),
            duration_ms: Some(50),
            start_time: Some(1700000000),
            session_id: Some(12345),
            prev_cmd: None,
            prev2_cmd: None,
        };
        db.store_command(&params1).unwrap();

        // Store second command with previous
        let params2 = StoreParams {
            cmd: "git commit -m 'test'".to_string(),
            cwd: "/home/user/project".to_string(),
            exit_status: Some(0),
            duration_ms: Some(200),
            start_time: Some(1700000001),
            session_id: Some(12345),
            prev_cmd: Some("git add -A".to_string()),
            prev2_cmd: None,
        };
        db.store_command(&params2).unwrap();

        // Check that bigram was created
        let conn = db.conn.lock().unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM ngrams_2", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_predictions() {
        let db = Database::open_in_memory().unwrap();

        // Store some commands
        for cmd in &["git status", "git add -A", "git commit -m 'test'", "git push"] {
            let params = StoreParams {
                cmd: cmd.to_string(),
                cwd: "/home/user/project".to_string(),
                exit_status: Some(0),
                duration_ms: Some(100),
                start_time: None,
                session_id: None,
                prev_cmd: None,
                prev2_cmd: None,
            };
            db.store_command(&params).unwrap();
        }

        // Get predictions for "git "
        let predict_params = PredictParams {
            prefix: "git".to_string(),
            cwd: "/home/user/project".to_string(),
            last_cmds: vec![],
            limit: 5,
            frecent_boost: true,
            weights: None,
        };

        let suggestions = db.predict(&predict_params).unwrap();
        assert!(!suggestions.is_empty());
        assert!(suggestions.iter().all(|s| s.cmd.starts_with("git")));
    }

    #[test]
    fn test_frecent_add_and_query() {
        let db = Database::open_in_memory().unwrap();

        // Add some directories
        for _ in 0..5 {
            db.frecent_add(&crate::protocol::FrecentAddParams {
                path: "/home/user/project".to_string(),
                path_type: "d".to_string(),
                rank: None,
                timestamp: None,
            }).unwrap();
        }

        db.frecent_add(&crate::protocol::FrecentAddParams {
            path: "/home/user/other".to_string(),
            path_type: "d".to_string(),
            rank: None,
            timestamp: None,
        }).unwrap();

        // Query without terms should return all sorted by score
        let results = db.frecent_query(&crate::protocol::FrecentQueryParams {
            terms: vec![],
            path_type: Some("d".to_string()),
            limit: 10,
            raw: false,
        }).unwrap();

        assert!(!results.is_empty());
        assert_eq!(results[0].path, "/home/user/project");
    }

    #[test]
    fn test_frecent_query_matching() {
        let db = Database::open_in_memory().unwrap();

        db.frecent_add(&crate::protocol::FrecentAddParams {
            path: "/home/user/project/src".to_string(),
            path_type: "d".to_string(),
            rank: None,
            timestamp: None,
        }).unwrap();

        db.frecent_add(&crate::protocol::FrecentAddParams {
            path: "/home/user/other".to_string(),
            path_type: "d".to_string(),
            rank: None,
            timestamp: None,
        }).unwrap();

        // Substring match
        let results = db.frecent_query(&crate::protocol::FrecentQueryParams {
            terms: vec!["proj".to_string(), "src".to_string()],
            path_type: Some("d".to_string()),
            limit: 10,
            raw: false,
        }).unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].path, "/home/user/project/src");
    }

    #[test]
    fn test_frecent_import_mode() {
        let db = Database::open_in_memory().unwrap();

        // Import with explicit rank/timestamp
        db.frecent_add(&crate::protocol::FrecentAddParams {
            path: "/imported/path".to_string(),
            path_type: "d".to_string(),
            rank: Some(42.5),
            timestamp: Some(1700000000),
        }).unwrap();

        let results = db.frecent_query(&crate::protocol::FrecentQueryParams {
            terms: vec!["imported".to_string()],
            path_type: None,
            limit: 10,
            raw: false,
        }).unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].path, "/imported/path");
    }

    #[test]
    fn test_frecency_score_function() {
        let now = 1700000000i64;

        // Recent access (within hour) should score higher
        let recent_score = frecency_score(10.0, now - 100, now);
        let day_old_score = frecency_score(10.0, now - 50000, now);
        let week_old_score = frecency_score(10.0, now - 400000, now);
        let old_score = frecency_score(10.0, now - 1000000, now);

        assert!(recent_score > day_old_score);
        assert!(day_old_score > week_old_score);
        assert!(week_old_score > old_score);
    }

    #[test]
    fn test_matches_ordered_substring_fn() {
        assert!(matches_ordered_substring("/home/user/project/src", &["proj".to_string(), "src".to_string()], false));
        assert!(!matches_ordered_substring("/home/user/project/src", &["src".to_string(), "proj".to_string()], false));
        // Case insensitive
        assert!(matches_ordered_substring("/Home/User/Project", &["project".to_string()], true));
        assert!(!matches_ordered_substring("/Home/User/Project", &["project".to_string()], false));
    }

    #[test]
    fn test_matches_fuzzy_fn() {
        assert!(matches_fuzzy("/home/user/project", &["prj".to_string()]));
        assert!(!matches_fuzzy("/home/user/project", &["xyz".to_string()]));
    }

    #[test]
    fn test_predictions_with_ngrams() {
        let db = Database::open_in_memory().unwrap();

        // Store git add followed by git commit multiple times
        for i in 0..5 {
            let params1 = StoreParams {
                cmd: "git add -A".to_string(),
                cwd: "/home/user/project".to_string(),
                exit_status: Some(0),
                duration_ms: Some(50),
                start_time: Some(1700000000 + i * 10),
                session_id: Some(12345),
                prev_cmd: None,
                prev2_cmd: None,
            };
            db.store_command(&params1).unwrap();

            let params2 = StoreParams {
                cmd: "git commit -m 'test'".to_string(),
                cwd: "/home/user/project".to_string(),
                exit_status: Some(0),
                duration_ms: Some(200),
                start_time: Some(1700000001 + i * 10),
                session_id: Some(12345),
                prev_cmd: Some("git add -A".to_string()),
                prev2_cmd: None,
            };
            db.store_command(&params2).unwrap();
        }

        // Get predictions after git add
        let predict_params = PredictParams {
            prefix: "git".to_string(),
            cwd: "/home/user/project".to_string(),
            last_cmds: vec!["git add -A".to_string()],
            limit: 5,
            frecent_boost: true,
            weights: None,
        };

        let suggestions = db.predict(&predict_params).unwrap();
        assert!(!suggestions.is_empty());
        // git commit should be highly ranked due to n-gram
        assert!(suggestions.iter().any(|s| s.cmd.contains("commit")));
    }

    #[test]
    fn test_ngram_ranks_successor_first() {
        let db = Database::open_in_memory().unwrap();

        // Store "make build" followed by "make test" many times (strong bigram)
        for i in 0..10 {
            db.store_command(&StoreParams {
                cmd: "make build".to_string(),
                cwd: "/home/user/project".to_string(),
                exit_status: Some(0),
                duration_ms: Some(50),
                start_time: Some(1700000000 + i * 10),
                session_id: Some(1),
                prev_cmd: None,
                prev2_cmd: None,
            }).unwrap();

            db.store_command(&StoreParams {
                cmd: "make test".to_string(),
                cwd: "/home/user/project".to_string(),
                exit_status: Some(0),
                duration_ms: Some(200),
                start_time: Some(1700000005 + i * 10),
                session_id: Some(1),
                prev_cmd: Some("make build".to_string()),
                prev2_cmd: None,
            }).unwrap();
        }

        // Also store "make clean" a few times (no bigram relationship with make build)
        for i in 0..3 {
            db.store_command(&StoreParams {
                cmd: "make clean".to_string(),
                cwd: "/home/user/project".to_string(),
                exit_status: Some(0),
                duration_ms: Some(30),
                start_time: Some(1700000200 + i * 10),
                session_id: Some(1),
                prev_cmd: None,
                prev2_cmd: None,
            }).unwrap();
        }

        // Predict with "make" prefix after "make build" — n-gram should push "make test" to top
        let suggestions = db.predict(&PredictParams {
            prefix: "make".to_string(),
            cwd: "/home/user/project".to_string(),
            last_cmds: vec!["make build".to_string()],
            limit: 5,
            frecent_boost: false,
            weights: None,
        }).unwrap();

        assert!(suggestions.len() >= 2, "Expected at least 2 suggestions, got {}", suggestions.len());
        assert_eq!(suggestions[0].cmd, "make test",
            "Expected 'make test' first due to bigram, got: {:?}", suggestions);

        // Verify n-gram score is meaningful (frequency=10, ln(10)/10 = 0.23)
        assert!(suggestions[0].score > 0.1,
            "N-gram score too low: {}", suggestions[0].score);

        // Verify the n-gram weight is applied
        let ngram_cmd = &suggestions[0];
        let non_ngram_cmd = suggestions.iter().find(|s| s.cmd != "make test").unwrap();
        assert!(ngram_cmd.score > non_ngram_cmd.score,
            "N-gram command should outscore non-n-gram: {} vs {}", ngram_cmd.score, non_ngram_cmd.score);

        // Without n-gram context, "make test" should NOT necessarily be first
        let suggestions_no_ngram = db.predict(&PredictParams {
            prefix: "make".to_string(),
            cwd: "/home/user/project".to_string(),
            last_cmds: vec![],
            limit: 5,
            frecent_boost: false,
            weights: None,
        }).unwrap();

        // All three make commands should appear
        let cmds: Vec<&str> = suggestions_no_ngram.iter().map(|s| s.cmd.as_str()).collect();
        assert!(cmds.contains(&"make build"), "Missing 'make build' in {:?}", cmds);
        assert!(cmds.contains(&"make test"), "Missing 'make test' in {:?}", cmds);
        assert!(cmds.contains(&"make clean"), "Missing 'make clean' in {:?}", cmds);
    }

    #[test]
    fn test_trigram_boosts_over_bigram() {
        let db = Database::open_in_memory().unwrap();

        // Build a chain: git add → git commit → git push (repeated)
        for i in 0..8 {
            db.store_command(&StoreParams {
                cmd: "git add -A".to_string(),
                cwd: "/home/user/project".to_string(),
                exit_status: Some(0), duration_ms: Some(50),
                start_time: Some(1700000000 + i * 30),
                session_id: Some(1),
                prev_cmd: None, prev2_cmd: None,
            }).unwrap();

            db.store_command(&StoreParams {
                cmd: "git commit -m 'wip'".to_string(),
                cwd: "/home/user/project".to_string(),
                exit_status: Some(0), duration_ms: Some(100),
                start_time: Some(1700000010 + i * 30),
                session_id: Some(1),
                prev_cmd: Some("git add -A".to_string()),
                prev2_cmd: None,
            }).unwrap();

            db.store_command(&StoreParams {
                cmd: "git push".to_string(),
                cwd: "/home/user/project".to_string(),
                exit_status: Some(0), duration_ms: Some(200),
                start_time: Some(1700000020 + i * 30),
                session_id: Some(1),
                prev_cmd: Some("git commit -m 'wip'".to_string()),
                prev2_cmd: Some("git add -A".to_string()),
            }).unwrap();
        }

        // Also store "git pull" after "git commit" a few times (bigram only, no trigram with add)
        for i in 0..3 {
            db.store_command(&StoreParams {
                cmd: "git pull".to_string(),
                cwd: "/home/user/project".to_string(),
                exit_status: Some(0), duration_ms: Some(150),
                start_time: Some(1700000300 + i * 10),
                session_id: Some(1),
                prev_cmd: Some("git commit -m 'wip'".to_string()),
                prev2_cmd: None,
            }).unwrap();
        }

        // With trigram context (add → commit → ?), "git push" should rank highest
        let suggestions = db.predict(&PredictParams {
            prefix: "git".to_string(),
            cwd: "/home/user/project".to_string(),
            last_cmds: vec![
                "git commit -m 'wip'".to_string(),
                "git add -A".to_string(),
            ],
            limit: 5,
            frecent_boost: false,
            weights: None,
        }).unwrap();

        // git push should benefit from both the trigram (add→commit→push) and bigram (commit→push)
        let push_entry = suggestions.iter().find(|s| s.cmd == "git push");
        assert!(push_entry.is_some(), "git push should appear in suggestions: {:?}", suggestions);

        let pull_entry = suggestions.iter().find(|s| s.cmd == "git pull");
        assert!(pull_entry.is_some(), "git pull should appear in suggestions: {:?}", suggestions);

        // Trigram-backed "git push" should outscore bigram-only "git pull"
        assert!(push_entry.unwrap().score > pull_entry.unwrap().score,
            "Trigram-backed 'git push' ({}) should outscore bigram-only 'git pull' ({})",
            push_entry.unwrap().score, pull_entry.unwrap().score);
    }
}
