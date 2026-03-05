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

        // Detect if command references local file arguments
        let has_local_file_args = Self::detect_local_file_args(&params.cmd, &params.cwd);

        // Insert history entry
        conn.execute(
            "INSERT INTO history (session_id, command_id, place_id, context_id, start_time, duration, exit_status, time_bucket, has_local_file_args)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            rusqlite::params![
                params.session_id,
                command_id,
                place_id,
                context_id,
                start_time,
                params.duration_ms.map(|d| d as f64 / 1000.0),
                params.exit_status,
                time_bucket,
                has_local_file_args as i32,
            ],
        )?;

        let history_id = conn.last_insert_rowid();

        // Update n-grams if previous command provided
        if let Some(ref prev_cmd) = params.prev_cmd {
            let prev_id = self.get_or_create_command(&conn, prev_cmd)?;
            self.update_bigram(&conn, prev_id, command_id)?;

            // Update exit-aware bigram if previous exit status provided
            if let Some(prev_exit) = params.prev_exit {
                let prev_exit_ok = if prev_exit == 0 { 1i32 } else { 0 };
                self.update_bigram_exit(&conn, prev_id, command_id, prev_exit_ok)?;
            }

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

    fn update_bigram_exit(
        &self,
        conn: &Connection,
        prev_id: i64,
        cmd_id: i64,
        prev_exit_ok: i32,
    ) -> Result<()> {
        let now = chrono_lite_timestamp();
        conn.execute(
            "INSERT INTO ngrams_2_exit (prev_command_id, command_id, prev_exit_ok, frequency, last_used)
             VALUES (?1, ?2, ?3, 1, ?4)
             ON CONFLICT(prev_command_id, command_id, prev_exit_ok) DO UPDATE SET
                frequency = frequency + 1,
                last_used = ?4",
            rusqlite::params![prev_id, cmd_id, prev_exit_ok, now],
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
        let w = params.weights.clone().unwrap_or_default();
        let ngram_bonus = self.compute_ngram_bonus(conn, &params.last_cmds, &params.prefix, params.limit, params.last_exit, &w)?;

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
                    CAST(SUM(CASE WHEN h.exit_status != 0 AND h.exit_status IS NOT NULL THEN 1 ELSE 0 END) AS REAL) / COUNT(*) as failure_rate,
                    MAX(h.has_local_file_args) as has_local_files
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
                row.get::<_, i32>(6).unwrap_or(0) != 0,
            ))
        })?;

        let now = chrono_lite_timestamp();

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
            if let Ok((cmd, freq, last_used, exact_dir_freq, hierarchy_score, failure_rate, has_local_files)) = row {
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
                let mut score = (freq_score * w.frequency + recency_score * w.recency + dir_score + frecent_boost + ngram_score).min(1.0) * failure_penalty;

                // Penalize commands with local file args when predicting from a different directory
                if has_local_files && exact_dir_freq == 0 {
                    score *= 1.0 - w.local_file_penalty;
                }

                suggestions.push(Suggestion { cmd, score });
            }
        }

        // Sort by score and limit
        suggestions.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
        suggestions.truncate(params.limit);

        Ok(suggestions)
    }

    /// Compute n-gram bonus scores for commands following the given previous commands.
    /// Returns a HashMap of command → bonus score (0.0-1.0).
    ///
    /// Uses conditional probability (freq/total_successors) instead of raw ln(freq),
    /// applies recency decay based on last_used timestamp, and optionally boosts
    /// exit-aware bigrams when last_exit is provided.
    fn compute_ngram_bonus(
        &self,
        conn: &Connection,
        last_cmds: &[String],
        prefix: &str,
        limit: usize,
        last_exit: Option<i32>,
        w: &crate::protocol::RankingWeights,
    ) -> Result<std::collections::HashMap<String, f64>> {
        let mut ngram_bonus: std::collections::HashMap<String, f64> = std::collections::HashMap::new();

        if last_cmds.is_empty() {
            return Ok(ngram_bonus);
        }

        let now = chrono_lite_timestamp();
        let halflife = w.ngram_recency_halflife;

        let prev1_cmd = &last_cmds[0];
        if let Ok(prev1_id) = self.get_command_id(conn, prev1_cmd) {
            // Trigram lookup: if we have two previous commands
            if last_cmds.len() >= 2 {
                let prev2_cmd = &last_cmds[1];
                if let Ok(prev2_id) = self.get_command_id(conn, prev2_cmd) {
                    // Fetch total trigram successor frequency for conditional probability
                    let total_trigram: f64 = conn.query_row(
                        "SELECT COALESCE(SUM(frequency), 0) FROM ngrams_3 WHERE prev2_command_id = ?1 AND prev1_command_id = ?2",
                        rusqlite::params![prev2_id, prev1_id],
                        |row| row.get(0),
                    ).unwrap_or(0.0);

                    if total_trigram > 0.0 {
                        let mut stmt = conn.prepare_cached(
                            "SELECT c.argv, n.frequency, n.last_used
                             FROM ngrams_3 n
                             JOIN commands c ON c.id = n.command_id
                             WHERE n.prev2_command_id = ?1 AND n.prev1_command_id = ?2
                               AND c.argv LIKE ?3 || '%'
                             ORDER BY n.frequency DESC
                             LIMIT ?4",
                        )?;

                        let rows = stmt.query_map(
                            rusqlite::params![prev2_id, prev1_id, prefix, limit],
                            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?, row.get::<_, i64>(2)?)),
                        )?;

                        for row in rows {
                            if let Ok((cmd, freq, last_used)) = row {
                                let cond_prob = freq as f64 / total_trigram;
                                let age_days = (now - last_used) as f64 / 86400.0;
                                let recency = (-age_days / halflife).exp();
                                let bonus = (cond_prob * recency * w.ngram_trigram_boost).min(1.0);
                                ngram_bonus.insert(cmd, bonus);
                            }
                        }
                    }
                }
            }

            // Exit-aware bigram lookup (if last_exit provided)
            // Track which commands got exit-aware scores so we can skip them in general bigram
            let mut exit_scored: std::collections::HashSet<String> = std::collections::HashSet::new();

            if let Some(exit_code) = last_exit {
                let prev_exit_ok = if exit_code == 0 { 1i32 } else { 0 };

                let total_exit: f64 = conn.query_row(
                    "SELECT COALESCE(SUM(frequency), 0) FROM ngrams_2_exit WHERE prev_command_id = ?1 AND prev_exit_ok = ?2",
                    rusqlite::params![prev1_id, prev_exit_ok],
                    |row| row.get(0),
                ).unwrap_or(0.0);

                if total_exit > 0.0 {
                    let mut stmt = conn.prepare_cached(
                        "SELECT c.argv, n.frequency, n.last_used
                         FROM ngrams_2_exit n
                         JOIN commands c ON c.id = n.command_id
                         WHERE n.prev_command_id = ?1 AND n.prev_exit_ok = ?2
                           AND c.argv LIKE ?3 || '%'
                         ORDER BY n.frequency DESC
                         LIMIT ?4",
                    )?;

                    let rows = stmt.query_map(
                        rusqlite::params![prev1_id, prev_exit_ok, prefix, limit],
                        |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?, row.get::<_, i64>(2)?)),
                    )?;

                    for row in rows {
                        if let Ok((cmd, freq, last_used)) = row {
                            let cond_prob = freq as f64 / total_exit;
                            let age_days = (now - last_used) as f64 / 86400.0;
                            let recency = (-age_days / halflife).exp();
                            let bonus = (cond_prob * recency * w.ngram_exit_boost).min(1.0);
                            // Only use exit-aware score if trigram didn't already provide a higher one
                            let current = ngram_bonus.get(&cmd).copied().unwrap_or(0.0);
                            if bonus > current {
                                ngram_bonus.insert(cmd.clone(), bonus);
                            }
                            exit_scored.insert(cmd);
                        }
                    }
                }
            }

            // General bigram lookup: prev1 → ?
            // Fetch total bigram successor frequency for conditional probability
            let total_bigram: f64 = conn.query_row(
                "SELECT COALESCE(SUM(frequency), 0) FROM ngrams_2 WHERE prev_command_id = ?1",
                rusqlite::params![prev1_id],
                |row| row.get(0),
            ).unwrap_or(0.0);

            if total_bigram > 0.0 {
                let mut stmt = conn.prepare_cached(
                    "SELECT c.argv, n.frequency, n.last_used
                     FROM ngrams_2 n
                     JOIN commands c ON c.id = n.command_id
                     WHERE n.prev_command_id = ?1 AND c.argv LIKE ?2 || '%'
                     ORDER BY n.frequency DESC
                     LIMIT ?3",
                )?;

                let rows = stmt.query_map(
                    rusqlite::params![prev1_id, prefix, limit],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?, row.get::<_, i64>(2)?)),
                )?;

                for row in rows {
                    if let Ok((cmd, freq, last_used)) = row {
                        // Skip if exit-aware data already scored this command
                        if exit_scored.contains(&cmd) {
                            continue;
                        }
                        let cond_prob = freq as f64 / total_bigram;
                        let age_days = (now - last_used) as f64 / 86400.0;
                        let recency = (-age_days / halflife).exp();
                        let bonus = (cond_prob * recency).min(1.0);
                        // Only insert if trigram didn't already provide a higher bonus
                        ngram_bonus.entry(cmd).or_insert(bonus);
                    }
                }
            }
        }

        Ok(ngram_bonus)
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

    /// Detect whether a command references local (relative) file paths that exist in cwd.
    /// Only relative paths trigger the flag — absolute/~ paths work from anywhere.
    fn detect_local_file_args(cmd: &str, cwd: &str) -> bool {
        use std::path::PathBuf;

        let parsed = parse_command(cmd);
        let mut checked = 0;

        for arg in &parsed.args {
            if checked >= 5 {
                break;
            }

            // Skip flags
            if arg.starts_with('-') {
                continue;
            }

            // Skip absolute paths and home-relative paths (they work from anywhere)
            if arg.starts_with('/') || arg.starts_with('~') {
                continue;
            }

            // Skip args that don't look like paths
            if !arg.contains('/') && !arg.contains('.') && arg.len() > 50 {
                continue;
            }

            checked += 1;

            if PathBuf::from(cwd).join(arg).exists() {
                return true;
            }
        }

        false
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
            "DELETE FROM ngrams_2_exit WHERE command_id = ?1 OR prev_command_id = ?1",
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

        // Compute n-gram bonuses if enabled
        let w = crate::protocol::RankingWeights::default();
        let ngram_bonus = if params.ngram_boost && !params.last_cmds.is_empty() {
            self.compute_ngram_bonus(&conn, &params.last_cmds, "", params.limit, params.last_exit, &w)?
        } else {
            std::collections::HashMap::new()
        };

        let cwd_for_query = params.cwd.clone().unwrap_or_default();
        let has_cwd = params.cwd.is_some();

        // No SQL LIMIT — we need all matching commands to score-sort properly.
        // GROUP BY c.id bounds results to unique commands (typically a few thousand),
        // and the aggregates (COUNT, SUM, MAX) require scanning all rows anyway.
        // Rust handles truncation to params.limit after score-sorting.
        let query = if params.dir.is_some() {
            "SELECT c.argv, p.dir, MAX(h.start_time) as last_used,
                    h.exit_status, h.duration,
                    COUNT(*) as cmd_freq,
                    CAST(SUM(CASE WHEN h.exit_status != 0 AND h.exit_status IS NOT NULL THEN 1 ELSE 0 END) AS REAL)
                        / COUNT(*) as failure_rate,
                    MAX(h.has_local_file_args) as has_local_files,
                    SUM(CASE WHEN p.dir = ?4 THEN 1 ELSE 0 END) as cwd_freq
             FROM history h
             JOIN commands c ON c.id = h.command_id
             JOIN places p ON p.id = h.place_id
             WHERE c.argv LIKE '%' || ?1 || '%'
               AND p.host = ?2
               AND p.dir = ?3
             GROUP BY c.id"
        } else {
            "SELECT c.argv, p.dir, MAX(h.start_time) as last_used,
                    h.exit_status, h.duration,
                    COUNT(*) as cmd_freq,
                    CAST(SUM(CASE WHEN h.exit_status != 0 AND h.exit_status IS NOT NULL THEN 1 ELSE 0 END) AS REAL)
                        / COUNT(*) as failure_rate,
                    MAX(h.has_local_file_args) as has_local_files,
                    SUM(CASE WHEN p.dir = ?3 THEN 1 ELSE 0 END) as cwd_freq
             FROM history h
             JOIN commands c ON c.id = h.command_id
             JOIN places p ON p.id = h.place_id
             WHERE c.argv LIKE '%' || ?1 || '%'
               AND p.host = ?2
             GROUP BY c.id"
        };

        let now = chrono_lite_timestamp();
        let ngram_weight = 0.40; // Same default as predict
        let mut stmt = conn.prepare(query)?;

        let map_row = |row: &rusqlite::Row| {
            let cmd: String = row.get(0)?;
            let timestamp: i64 = row.get(2)?;
            let exit_status: Option<i32> = row.get(3)?;
            let cmd_freq: i64 = row.get(5)?;
            let failure_rate: f64 = row.get::<_, f64>(6).unwrap_or(0.0);
            let has_local_files: bool = row.get::<_, i32>(7).unwrap_or(0) != 0;
            let cwd_freq: i64 = row.get::<_, i64>(8).unwrap_or(0);

            let age_days = (now - timestamp) as f64 / 86400.0;
            let recency_score = (-age_days / 30.0_f64).exp();
            let freq_score = (cmd_freq as f64).ln().max(0.0) / 10.0;
            let failure_penalty = 1.0 - (failure_rate * w.failure_penalty);

            // Apply n-gram bonus if available
            let ngram_score = ngram_bonus.get(&cmd).copied().unwrap_or(0.0) * ngram_weight;
            let mut score = (freq_score * w.frequency + recency_score * w.recency + ngram_score).min(1.0) * failure_penalty;

            // Penalize commands with local file args when searching from a different directory
            if has_local_files && cwd_freq == 0 && has_cwd {
                score *= 1.0 - w.local_file_penalty;
            }

            Ok(SearchResult {
                cmd,
                cwd: row.get(1)?,
                timestamp,
                exit_status,
                duration_ms: row.get::<_, Option<f64>>(4)?.map(|d| (d * 1000.0) as i64),
                score: Some(score),
            })
        };

        let mut results: Vec<SearchResult> = if let Some(ref dir) = params.dir {
            stmt.query_map(
                rusqlite::params![params.pattern, hostname, dir, cwd_for_query],
                map_row,
            )?
            .filter_map(|r| r.ok())
            .collect()
        } else {
            stmt.query_map(
                rusqlite::params![params.pattern, hostname, cwd_for_query],
                map_row,
            )?
            .filter_map(|r| r.ok())
            .collect()
        };

        // Sort by score descending and truncate to requested limit
        results.sort_by(|a, b| {
            b.score.unwrap_or(0.0).partial_cmp(&a.score.unwrap_or(0.0))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(params.limit);

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
            prev_exit: None,
        };

        let id = db.store_command(&params).unwrap();
        assert!(id > 0);

        // Search for the command
        let search_params = SearchParams {
            pattern: "git".to_string(),
            limit: 10,
            dir: None,
            exit_status: None,
            last_cmds: vec![],
            cwd: None,
            ngram_boost: false,
            last_exit: None,
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
            prev_exit: None,
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
            prev_exit: None,
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
                prev_exit: None,
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
            last_exit: None,
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
                prev_exit: None,
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
                prev_exit: None,
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
            last_exit: None,
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
                prev2_cmd: None, prev_exit: None,
            }).unwrap();

            db.store_command(&StoreParams {
                cmd: "make test".to_string(),
                cwd: "/home/user/project".to_string(),
                exit_status: Some(0),
                duration_ms: Some(200),
                start_time: Some(1700000005 + i * 10),
                session_id: Some(1),
                prev_cmd: Some("make build".to_string()),
                prev2_cmd: None, prev_exit: None,
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
                prev2_cmd: None, prev_exit: None,
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
            last_exit: None,
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
            last_exit: None,
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
                prev_cmd: None, prev2_cmd: None, prev_exit: None,
            }).unwrap();

            db.store_command(&StoreParams {
                cmd: "git commit -m 'wip'".to_string(),
                cwd: "/home/user/project".to_string(),
                exit_status: Some(0), duration_ms: Some(100),
                start_time: Some(1700000010 + i * 30),
                session_id: Some(1),
                prev_cmd: Some("git add -A".to_string()),
                prev2_cmd: None, prev_exit: None,
            }).unwrap();

            db.store_command(&StoreParams {
                cmd: "git push".to_string(),
                cwd: "/home/user/project".to_string(),
                exit_status: Some(0), duration_ms: Some(200),
                start_time: Some(1700000020 + i * 30),
                session_id: Some(1),
                prev_cmd: Some("git commit -m 'wip'".to_string()),
                prev2_cmd: Some("git add -A".to_string()),
                prev_exit: None,
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
                prev2_cmd: None, prev_exit: None,
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
            last_exit: None,
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

    #[test]
    fn test_search_deduplication() {
        let db = Database::open_in_memory().unwrap();

        // Store the same command 5 times
        for i in 0..5 {
            db.store_command(&StoreParams {
                cmd: "ls -la".to_string(),
                cwd: "/home/user".to_string(),
                exit_status: Some(0), duration_ms: Some(10),
                start_time: Some(1700000000 + i * 10),
                session_id: Some(1),
                prev_cmd: None, prev2_cmd: None, prev_exit: None,
            }).unwrap();
        }

        let results = db.search(&SearchParams {
            pattern: "ls".to_string(),
            limit: 10,
            dir: None, exit_status: None,
            last_cmds: vec![], cwd: None, ngram_boost: false, last_exit: None,
        }).unwrap();

        // Should return exactly 1 result, not 5
        assert_eq!(results.len(), 1, "Search should deduplicate: {:?}", results);
        assert_eq!(results[0].cmd, "ls -la");
    }

    #[test]
    fn test_search_score_ordering() {
        let db = Database::open_in_memory().unwrap();

        // Store "rare" once and "common" many times
        db.store_command(&StoreParams {
            cmd: "rare-cmd".to_string(),
            cwd: "/home/user".to_string(),
            exit_status: Some(0), duration_ms: Some(10),
            start_time: Some(1700000000),
            session_id: Some(1),
            prev_cmd: None, prev2_cmd: None, prev_exit: None,
        }).unwrap();

        for i in 0..20 {
            db.store_command(&StoreParams {
                cmd: "common-cmd".to_string(),
                cwd: "/home/user".to_string(),
                exit_status: Some(0), duration_ms: Some(10),
                start_time: Some(1700000000 + i),
                session_id: Some(1),
                prev_cmd: None, prev2_cmd: None, prev_exit: None,
            }).unwrap();
        }

        let results = db.search(&SearchParams {
            pattern: "cmd".to_string(),
            limit: 10,
            dir: None, exit_status: None,
            last_cmds: vec![], cwd: None, ngram_boost: false, last_exit: None,
        }).unwrap();

        assert!(results.len() >= 2);
        // Higher-scored result should come first
        assert!(results[0].score >= results[1].score,
            "Results should be sorted by score: {} >= {}", results[0].score.unwrap(), results[1].score.unwrap());
        // common-cmd should outscore rare-cmd (higher frequency)
        assert_eq!(results[0].cmd, "common-cmd");
    }

    #[test]
    fn test_search_ngram_boost() {
        let db = Database::open_in_memory().unwrap();

        // Build bigram: "cargo build" → "cargo test"
        for i in 0..10 {
            db.store_command(&StoreParams {
                cmd: "cargo build".to_string(),
                cwd: "/home/user".to_string(),
                exit_status: Some(0), duration_ms: Some(100),
                start_time: Some(1700000000 + i * 10),
                session_id: Some(1),
                prev_cmd: None, prev2_cmd: None, prev_exit: None,
            }).unwrap();
            db.store_command(&StoreParams {
                cmd: "cargo test".to_string(),
                cwd: "/home/user".to_string(),
                exit_status: Some(0), duration_ms: Some(200),
                start_time: Some(1700000005 + i * 10),
                session_id: Some(1),
                prev_cmd: Some("cargo build".to_string()),
                prev2_cmd: None, prev_exit: None,
            }).unwrap();
        }

        // Store "cargo doc" with same frequency but no bigram link
        for i in 0..10 {
            db.store_command(&StoreParams {
                cmd: "cargo doc".to_string(),
                cwd: "/home/user".to_string(),
                exit_status: Some(0), duration_ms: Some(100),
                start_time: Some(1700000000 + i * 10),
                session_id: Some(1),
                prev_cmd: None, prev2_cmd: None, prev_exit: None,
            }).unwrap();
        }

        // Search with ngram context: last cmd was "cargo build"
        let with_ngram = db.search(&SearchParams {
            pattern: "cargo".to_string(),
            limit: 10,
            dir: None, exit_status: None,
            last_cmds: vec!["cargo build".to_string()],
            cwd: None, ngram_boost: true, last_exit: None,
        }).unwrap();

        let test_entry = with_ngram.iter().find(|r| r.cmd == "cargo test").unwrap();
        let doc_entry = with_ngram.iter().find(|r| r.cmd == "cargo doc").unwrap();

        // "cargo test" should outscore "cargo doc" due to bigram bonus
        assert!(test_entry.score > doc_entry.score,
            "N-gram boosted 'cargo test' ({:?}) should outscore 'cargo doc' ({:?})",
            test_entry.score, doc_entry.score);
    }

    #[test]
    fn test_failure_penalty() {
        let db = Database::open_in_memory().unwrap();

        // Store a command that always succeeds
        for i in 0..10 {
            db.store_command(&StoreParams {
                cmd: "good-cmd".to_string(),
                cwd: "/home/user".to_string(),
                exit_status: Some(0), duration_ms: Some(10),
                start_time: Some(1700000000 + i),
                session_id: Some(1),
                prev_cmd: None, prev2_cmd: None, prev_exit: None,
            }).unwrap();
        }

        // Store a command that always fails
        for i in 0..10 {
            db.store_command(&StoreParams {
                cmd: "bad-cmd".to_string(),
                cwd: "/home/user".to_string(),
                exit_status: Some(1), duration_ms: Some(10),
                start_time: Some(1700000000 + i),
                session_id: Some(1),
                prev_cmd: None, prev2_cmd: None, prev_exit: None,
            }).unwrap();
        }

        let results = db.predict(&PredictParams {
            prefix: "".to_string(),
            cwd: "/home/user".to_string(),
            last_cmds: vec![],
            limit: 10,
            frecent_boost: false,
            weights: None,
            last_exit: None,
        }).unwrap();

        let good = results.iter().find(|s| s.cmd == "good-cmd");
        let bad = results.iter().find(|s| s.cmd == "bad-cmd");
        assert!(good.is_some() && bad.is_some(), "Both commands should appear");
        assert!(good.unwrap().score > bad.unwrap().score,
            "Failing command should score lower: good={} bad={}",
            good.unwrap().score, bad.unwrap().score);
    }

    #[test]
    fn test_delete_command() {
        let db = Database::open_in_memory().unwrap();

        db.store_command(&StoreParams {
            cmd: "secret-cmd".to_string(),
            cwd: "/home/user".to_string(),
            exit_status: Some(0), duration_ms: Some(10),
            start_time: Some(1700000000),
            session_id: Some(1),
            prev_cmd: None, prev2_cmd: None, prev_exit: None,
        }).unwrap();

        // Verify it exists
        let results = db.search(&SearchParams {
            pattern: "secret".to_string(),
            limit: 10,
            dir: None, exit_status: None,
            last_cmds: vec![], cwd: None, ngram_boost: false, last_exit: None,
        }).unwrap();
        assert_eq!(results.len(), 1);

        // Delete it
        let deleted = db.delete_command("secret-cmd").unwrap();
        assert_eq!(deleted, 1);

        // Verify it's gone
        let results = db.search(&SearchParams {
            pattern: "secret".to_string(),
            limit: 10,
            dir: None, exit_status: None,
            last_cmds: vec![], cwd: None, ngram_boost: false, last_exit: None,
        }).unwrap();
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_custom_ranking_weights() {
        let db = Database::open_in_memory().unwrap();

        // Store commands with different characteristics
        for i in 0..10 {
            db.store_command(&StoreParams {
                cmd: "frequent-cmd".to_string(),
                cwd: "/home/user".to_string(),
                exit_status: Some(0), duration_ms: Some(10),
                start_time: Some(1700000000 + i),
                session_id: Some(1),
                prev_cmd: None, prev2_cmd: None, prev_exit: None,
            }).unwrap();
        }
        db.store_command(&StoreParams {
            cmd: "recent-cmd".to_string(),
            cwd: "/home/user".to_string(),
            exit_status: Some(0), duration_ms: Some(10),
            start_time: None, // defaults to now
            session_id: Some(1),
            prev_cmd: None, prev2_cmd: None, prev_exit: None,
        }).unwrap();

        // With high frequency weight, frequent-cmd should win
        let freq_heavy = db.predict(&PredictParams {
            prefix: "".to_string(),
            cwd: "/home/user".to_string(),
            last_cmds: vec![],
            limit: 10,
            frecent_boost: false,
            weights: Some(crate::protocol::RankingWeights {
                frequency: 1.0,
                recency: 0.0,
                ngram: 0.0,
                dir_exact: 0.0,
                dir_hierarchy: 0.0,
                failure_penalty: 0.0,
                frecent_boost_max: 0.0,
                ngram_recency_halflife: 60.0,
                ngram_trigram_boost: 1.5,
                ngram_exit_boost: 1.2,
                local_file_penalty: 0.0,
            }),
            last_exit: None,
        }).unwrap();

        assert!(!freq_heavy.is_empty());
        assert_eq!(freq_heavy[0].cmd, "frequent-cmd",
            "With frequency=1.0, recency=0.0, frequent-cmd should be first: {:?}", freq_heavy);
    }

    #[test]
    fn test_frecent_aging() {
        let db = Database::open_in_memory().unwrap();

        // Add many paths with high ranks to trigger aging (total > 2000)
        for i in 0..50 {
            db.frecent_add(&FrecentAddParams {
                path: format!("/path/{}", i),
                path_type: "d".to_string(),
                rank: Some(50.0), // 50 * 50 = 2500 total
                timestamp: Some(1700000000),
            }).unwrap();
        }

        // Add one more to trigger aging check
        db.frecent_add(&FrecentAddParams {
            path: "/path/trigger".to_string(),
            path_type: "d".to_string(),
            rank: None,
            timestamp: None,
        }).unwrap();

        // After aging, ranks should be decayed (multiplied by 0.9)
        let results = db.frecent_query(&FrecentQueryParams {
            terms: vec!["path/0".to_string()],
            path_type: Some("d".to_string()),
            limit: 1,
            raw: true,
        }).unwrap();

        assert!(!results.is_empty());
        // Original rank was 50.0, after 0.9x decay should be 45.0
        assert!(results[0].rank.unwrap() < 50.0,
            "Rank should be decayed after aging: {}", results[0].rank.unwrap());
    }

    #[test]
    fn test_arg_suggestions() {
        let db = Database::open_in_memory().unwrap();

        // Store git checkout with branch args multiple times
        for i in 0..5 {
            db.store_command(&StoreParams {
                cmd: "git checkout main".to_string(),
                cwd: "/home/user/project".to_string(),
                exit_status: Some(0), duration_ms: Some(50),
                start_time: Some(1700000000 + i * 10),
                session_id: Some(1),
                prev_cmd: None, prev2_cmd: None, prev_exit: None,
            }).unwrap();
        }
        for i in 0..3 {
            db.store_command(&StoreParams {
                cmd: "git checkout develop".to_string(),
                cwd: "/home/user/project".to_string(),
                exit_status: Some(0), duration_ms: Some(50),
                start_time: Some(1700000100 + i * 10),
                session_id: Some(1),
                prev_cmd: None, prev2_cmd: None, prev_exit: None,
            }).unwrap();
        }

        // Ask for arg suggestions for "git checkout "
        let suggestions = db.get_arg_suggestions(
            "git checkout ", "/home/user/project", 5,
        ).unwrap();

        assert!(!suggestions.is_empty(), "Should suggest branch names");
        // All suggestions should start with "git checkout "
        assert!(suggestions.iter().all(|s| s.cmd.starts_with("git checkout ")),
            "All suggestions should complete the command: {:?}", suggestions);
    }

    #[test]
    fn test_local_file_penalty_search() {
        let db = Database::open_in_memory().unwrap();

        // Store a command with local file args from dir-a
        db.store_command(&StoreParams {
            cmd: "vim foo.py".to_string(),
            cwd: "/home/user/dir-a".to_string(),
            exit_status: Some(0), duration_ms: Some(50),
            start_time: Some(1700000000),
            session_id: Some(1),
            prev_cmd: None, prev2_cmd: None, prev_exit: None,
        }).unwrap();

        // Store a generic command from dir-a
        db.store_command(&StoreParams {
            cmd: "vim --version".to_string(),
            cwd: "/home/user/dir-a".to_string(),
            exit_status: Some(0), duration_ms: Some(50),
            start_time: Some(1700000000),
            session_id: Some(1),
            prev_cmd: None, prev2_cmd: None, prev_exit: None,
        }).unwrap();

        // Manually set has_local_file_args on the first command
        {
            let conn = db.conn.lock().unwrap();
            conn.execute(
                "UPDATE history SET has_local_file_args = 1 WHERE command_id = (SELECT id FROM commands WHERE argv = 'vim foo.py')",
                [],
            ).unwrap();
        }

        // Search from dir-b: command with local files should be penalized
        let results = db.search(&SearchParams {
            pattern: "vim".to_string(),
            limit: 10,
            dir: None,
            exit_status: None,
            last_cmds: vec![],
            cwd: Some("/home/user/dir-b".to_string()),
            ngram_boost: false,
            last_exit: None,
        }).unwrap();

        assert_eq!(results.len(), 2);
        let local_score = results.iter().find(|r| r.cmd == "vim foo.py").unwrap().score.unwrap();
        let generic_score = results.iter().find(|r| r.cmd == "vim --version").unwrap().score.unwrap();
        assert!(local_score < generic_score,
            "Local file command should score lower from different dir: {:.4} vs {:.4}", local_score, generic_score);
    }

    #[test]
    fn test_local_file_no_penalty_same_dir() {
        let db = Database::open_in_memory().unwrap();

        // Store a command with local file args
        db.store_command(&StoreParams {
            cmd: "vim foo.py".to_string(),
            cwd: "/home/user/dir-a".to_string(),
            exit_status: Some(0), duration_ms: Some(50),
            start_time: Some(1700000000),
            session_id: Some(1),
            prev_cmd: None, prev2_cmd: None, prev_exit: None,
        }).unwrap();

        // Store same command without local files for comparison
        db.store_command(&StoreParams {
            cmd: "vim --version".to_string(),
            cwd: "/home/user/dir-a".to_string(),
            exit_status: Some(0), duration_ms: Some(50),
            start_time: Some(1700000000),
            session_id: Some(1),
            prev_cmd: None, prev2_cmd: None, prev_exit: None,
        }).unwrap();

        // Set has_local_file_args
        {
            let conn = db.conn.lock().unwrap();
            conn.execute(
                "UPDATE history SET has_local_file_args = 1 WHERE command_id = (SELECT id FROM commands WHERE argv = 'vim foo.py')",
                [],
            ).unwrap();
        }

        // Search from SAME dir: no penalty should apply
        let results = db.search(&SearchParams {
            pattern: "vim".to_string(),
            limit: 10,
            dir: None,
            exit_status: None,
            last_cmds: vec![],
            cwd: Some("/home/user/dir-a".to_string()),
            ngram_boost: false,
            last_exit: None,
        }).unwrap();

        let local_score = results.iter().find(|r| r.cmd == "vim foo.py").unwrap().score.unwrap();
        let generic_score = results.iter().find(|r| r.cmd == "vim --version").unwrap().score.unwrap();
        // Same timestamps, same freq — scores should be equal (no penalty from same dir)
        assert!((local_score - generic_score).abs() < 0.001,
            "No penalty from same dir: {:.4} vs {:.4}", local_score, generic_score);
    }

    #[test]
    fn test_local_file_penalty_predict() {
        let db = Database::open_in_memory().unwrap();

        // Store commands from dir-a
        db.store_command(&StoreParams {
            cmd: "vim foo.py".to_string(),
            cwd: "/home/user/dir-a".to_string(),
            exit_status: Some(0), duration_ms: Some(50),
            start_time: Some(1700000000),
            session_id: Some(1),
            prev_cmd: None, prev2_cmd: None, prev_exit: None,
        }).unwrap();

        db.store_command(&StoreParams {
            cmd: "vim --version".to_string(),
            cwd: "/home/user/dir-a".to_string(),
            exit_status: Some(0), duration_ms: Some(50),
            start_time: Some(1700000000),
            session_id: Some(1),
            prev_cmd: None, prev2_cmd: None, prev_exit: None,
        }).unwrap();

        // Set has_local_file_args
        {
            let conn = db.conn.lock().unwrap();
            conn.execute(
                "UPDATE history SET has_local_file_args = 1 WHERE command_id = (SELECT id FROM commands WHERE argv = 'vim foo.py')",
                [],
            ).unwrap();
        }

        // Predict from dir-b: command with local files should be penalized
        let suggestions = db.predict(&PredictParams {
            prefix: "vim".to_string(),
            cwd: "/home/user/dir-b".to_string(),
            last_cmds: vec![],
            limit: 10,
            frecent_boost: false,
            weights: None,
            last_exit: None,
        }).unwrap();

        assert!(suggestions.len() >= 2);
        let local_score = suggestions.iter().find(|s| s.cmd == "vim foo.py").unwrap().score;
        let generic_score = suggestions.iter().find(|s| s.cmd == "vim --version").unwrap().score;
        assert!(local_score < generic_score,
            "Local file command should score lower in predict from different dir: {:.4} vs {:.4}", local_score, generic_score);
    }

    #[test]
    fn test_conditional_probability_scoring() {
        let db = Database::open_in_memory().unwrap();

        // prev_A → cmd_target 8 times out of 10 total successors (80%)
        for i in 0..8 {
            db.store_command(&StoreParams {
                cmd: "cmd_target".to_string(),
                cwd: "/home/user".to_string(),
                exit_status: Some(0), duration_ms: Some(10),
                start_time: Some(1700000000 + i),
                session_id: Some(1),
                prev_cmd: Some("prev_A".to_string()),
                prev2_cmd: None, prev_exit: None,
            }).unwrap();
        }
        for i in 0..2 {
            db.store_command(&StoreParams {
                cmd: "cmd_other_a".to_string(),
                cwd: "/home/user".to_string(),
                exit_status: Some(0), duration_ms: Some(10),
                start_time: Some(1700000010 + i),
                session_id: Some(1),
                prev_cmd: Some("prev_A".to_string()),
                prev2_cmd: None, prev_exit: None,
            }).unwrap();
        }

        // prev_B → cmd_target 8 times out of 100 total successors (8%)
        for i in 0..8 {
            db.store_command(&StoreParams {
                cmd: "cmd_target".to_string(),
                cwd: "/home/user".to_string(),
                exit_status: Some(0), duration_ms: Some(10),
                start_time: Some(1700000100 + i),
                session_id: Some(1),
                prev_cmd: Some("prev_B".to_string()),
                prev2_cmd: None, prev_exit: None,
            }).unwrap();
        }
        for i in 0..92 {
            db.store_command(&StoreParams {
                cmd: format!("cmd_noise_{}", i),
                cwd: "/home/user".to_string(),
                exit_status: Some(0), duration_ms: Some(10),
                start_time: Some(1700000200 + i as i64),
                session_id: Some(1),
                prev_cmd: Some("prev_B".to_string()),
                prev2_cmd: None, prev_exit: None,
            }).unwrap();
        }

        // Store prev_A and prev_B themselves
        db.store_command(&StoreParams {
            cmd: "prev_A".to_string(), cwd: "/home/user".to_string(),
            exit_status: Some(0), duration_ms: Some(10),
            start_time: Some(1700000000), session_id: Some(1),
            prev_cmd: None, prev2_cmd: None, prev_exit: None,
        }).unwrap();
        db.store_command(&StoreParams {
            cmd: "prev_B".to_string(), cwd: "/home/user".to_string(),
            exit_status: Some(0), duration_ms: Some(10),
            start_time: Some(1700000000), session_id: Some(1),
            prev_cmd: None, prev2_cmd: None, prev_exit: None,
        }).unwrap();

        let w = crate::protocol::RankingWeights::default();
        let conn = db.conn.lock().unwrap();

        // After prev_A: cmd_target should have ~0.8 conditional probability
        let bonus_a = db.compute_ngram_bonus(&conn, &["prev_A".to_string()], "cmd_target", 10, None, &w).unwrap();
        let score_a = bonus_a.get("cmd_target").copied().unwrap_or(0.0);

        // After prev_B: cmd_target should have ~0.08 conditional probability
        let bonus_b = db.compute_ngram_bonus(&conn, &["prev_B".to_string()], "cmd_target", 10, None, &w).unwrap();
        let score_b = bonus_b.get("cmd_target").copied().unwrap_or(0.0);

        // 80% conditional probability should significantly outscore 8%
        assert!(score_a > score_b * 2.0,
            "Conditional prob: prev_A→target ({}) should be >> prev_B→target ({})",
            score_a, score_b);
    }

    #[test]
    fn test_ngram_recency_decay() {
        let db = Database::open_in_memory().unwrap();
        let now = chrono_lite_timestamp();

        // Store prev_cmd
        db.store_command(&StoreParams {
            cmd: "prev_cmd".to_string(), cwd: "/home/user".to_string(),
            exit_status: Some(0), duration_ms: Some(10),
            start_time: Some(now), session_id: Some(1),
            prev_cmd: None, prev2_cmd: None, prev_exit: None,
        }).unwrap();

        let conn = db.conn.lock().unwrap();
        let prev_id = db.get_command_id(&conn, "prev_cmd").unwrap();

        // Insert a recent bigram (today)
        let recent_id = db.get_or_create_command(&conn, "recent_successor").unwrap();
        conn.execute(
            "INSERT INTO ngrams_2 (prev_command_id, command_id, frequency, last_used) VALUES (?1, ?2, 5, ?3)",
            rusqlite::params![prev_id, recent_id, now],
        ).unwrap();

        // Insert a stale bigram (180 days ago, same frequency)
        let stale_id = db.get_or_create_command(&conn, "stale_successor").unwrap();
        let stale_time = now - 180 * 86400;
        conn.execute(
            "INSERT INTO ngrams_2 (prev_command_id, command_id, frequency, last_used) VALUES (?1, ?2, 5, ?3)",
            rusqlite::params![prev_id, stale_id, stale_time],
        ).unwrap();

        let w = crate::protocol::RankingWeights::default();
        let bonus = db.compute_ngram_bonus(&conn, &["prev_cmd".to_string()], "", 10, None, &w).unwrap();

        let recent_score = bonus.get("recent_successor").copied().unwrap_or(0.0);
        let stale_score = bonus.get("stale_successor").copied().unwrap_or(0.0);

        assert!(recent_score > stale_score * 2.0,
            "Recent bigram ({}) should significantly outscore stale bigram ({})",
            recent_score, stale_score);
    }

    #[test]
    fn test_exit_aware_bigram() {
        let db = Database::open_in_memory().unwrap();

        // After "make" fails → "make clean" (10 times)
        for i in 0..10 {
            db.store_command(&StoreParams {
                cmd: "make clean".to_string(),
                cwd: "/home/user".to_string(),
                exit_status: Some(0), duration_ms: Some(10),
                start_time: Some(1700000000 + i),
                session_id: Some(1),
                prev_cmd: Some("make".to_string()),
                prev2_cmd: None,
                prev_exit: Some(2), // make failed
            }).unwrap();
        }

        // After "make" succeeds → "make install" (10 times)
        for i in 0..10 {
            db.store_command(&StoreParams {
                cmd: "make install".to_string(),
                cwd: "/home/user".to_string(),
                exit_status: Some(0), duration_ms: Some(10),
                start_time: Some(1700000100 + i),
                session_id: Some(1),
                prev_cmd: Some("make".to_string()),
                prev2_cmd: None,
                prev_exit: Some(0), // make succeeded
            }).unwrap();
        }

        // Store "make" itself
        db.store_command(&StoreParams {
            cmd: "make".to_string(), cwd: "/home/user".to_string(),
            exit_status: Some(0), duration_ms: Some(10),
            start_time: Some(1700000000), session_id: Some(1),
            prev_cmd: None, prev2_cmd: None, prev_exit: None,
        }).unwrap();

        let w = crate::protocol::RankingWeights::default();
        let conn = db.conn.lock().unwrap();

        // After "make" failed: "make clean" should be boosted
        let bonus_fail = db.compute_ngram_bonus(
            &conn, &["make".to_string()], "make", 10, Some(2), &w,
        ).unwrap();
        let clean_after_fail = bonus_fail.get("make clean").copied().unwrap_or(0.0);
        let install_after_fail = bonus_fail.get("make install").copied().unwrap_or(0.0);

        assert!(clean_after_fail > install_after_fail,
            "After make fails, 'make clean' ({}) should outscore 'make install' ({})",
            clean_after_fail, install_after_fail);

        // After "make" succeeded: "make install" should be boosted
        let bonus_ok = db.compute_ngram_bonus(
            &conn, &["make".to_string()], "make", 10, Some(0), &w,
        ).unwrap();
        let install_after_ok = bonus_ok.get("make install").copied().unwrap_or(0.0);
        let clean_after_ok = bonus_ok.get("make clean").copied().unwrap_or(0.0);

        assert!(install_after_ok > clean_after_ok,
            "After make succeeds, 'make install' ({}) should outscore 'make clean' ({})",
            install_after_ok, clean_after_ok);
    }
}
