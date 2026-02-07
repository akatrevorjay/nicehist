//! Database migrations for schema versioning.

use anyhow::{Context, Result};
use rusqlite::Connection;
use tracing::info;

use super::schema::SCHEMA_V1;

/// Current schema version
const CURRENT_VERSION: i32 = 3;

/// Run all pending migrations
pub fn run_migrations(conn: &Connection) -> Result<()> {
    let current = get_schema_version(conn)?;

    if current == 0 {
        // Fresh database, apply initial schema (includes all tables up to current version)
        info!("Applying initial schema (version {})", CURRENT_VERSION);
        conn.execute_batch(SCHEMA_V1)
            .context("Failed to apply initial schema")?;
        set_schema_version(conn, CURRENT_VERSION)?;
    } else if current < CURRENT_VERSION {
        // Apply incremental migrations
        for version in (current + 1)..=CURRENT_VERSION {
            info!("Applying migration to version {}", version);
            apply_migration(conn, version)?;
            set_schema_version(conn, version)?;
        }
    }

    Ok(())
}

/// Get current schema version (0 if not initialized)
fn get_schema_version(conn: &Connection) -> Result<i32> {
    // Check if schema_version table exists
    let table_exists: bool = conn
        .query_row(
            "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='schema_version'",
            [],
            |row| row.get(0),
        )
        .unwrap_or(false);

    if !table_exists {
        return Ok(0);
    }

    let version: i32 = conn
        .query_row(
            "SELECT COALESCE(MAX(version), 0) FROM schema_version",
            [],
            |row| row.get(0),
        )
        .unwrap_or(0);

    Ok(version)
}

/// Set schema version
fn set_schema_version(conn: &Connection, version: i32) -> Result<()> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    conn.execute(
        "INSERT OR REPLACE INTO schema_version (version, applied_at) VALUES (?1, ?2)",
        [version as i64, now],
    )?;

    Ok(())
}

/// Apply a specific migration
fn apply_migration(conn: &Connection, version: i32) -> Result<()> {
    match version {
        2 => apply_migration_v2(conn),
        3 => apply_migration_v3(conn),
        _ => Ok(()), // No migration needed
    }
}

/// Migration v2: Add parsed_commands and arg_patterns tables for argument-aware suggestions
fn apply_migration_v2(conn: &Connection) -> Result<()> {
    conn.execute_batch(r#"
        -- Parsed commands for argument-aware suggestions
        CREATE TABLE IF NOT EXISTS parsed_commands (
            command_id INTEGER PRIMARY KEY REFERENCES commands(id),
            program TEXT NOT NULL,
            subcommand TEXT,
            args_hash TEXT
        );

        -- Argument patterns: what arguments follow a given program+subcommand
        CREATE TABLE IF NOT EXISTS arg_patterns (
            id INTEGER PRIMARY KEY,
            program TEXT NOT NULL,
            subcommand TEXT,
            arg_value TEXT NOT NULL,
            frequency INTEGER NOT NULL DEFAULT 1,
            last_used INTEGER NOT NULL,
            place_id INTEGER REFERENCES places(id),
            UNIQUE(program, subcommand, arg_value, place_id)
        );

        -- Indexes for fast queries
        CREATE INDEX IF NOT EXISTS idx_parsed_commands_program ON parsed_commands(program);
        CREATE INDEX IF NOT EXISTS idx_parsed_commands_subcommand ON parsed_commands(program, subcommand);
        CREATE INDEX IF NOT EXISTS idx_arg_patterns_lookup ON arg_patterns(program, subcommand);
        CREATE INDEX IF NOT EXISTS idx_arg_patterns_place ON arg_patterns(place_id);
    "#).context("Failed to apply migration v2")?;

    Ok(())
}

/// Migration v3: Add frecent_paths table for fasd-like frecency tracking
fn apply_migration_v3(conn: &Connection) -> Result<()> {
    conn.execute_batch(r#"
        -- Frecent paths (fasd-like frecency tracking)
        CREATE TABLE IF NOT EXISTS frecent_paths (
            id INTEGER PRIMARY KEY,
            path TEXT NOT NULL,
            path_type TEXT NOT NULL DEFAULT 'd',
            rank REAL NOT NULL DEFAULT 1.0,
            last_access INTEGER NOT NULL,
            access_count INTEGER NOT NULL DEFAULT 1,
            UNIQUE(path, path_type)
        );

        CREATE INDEX IF NOT EXISTS idx_frecent_paths_type ON frecent_paths(path_type);
        CREATE INDEX IF NOT EXISTS idx_frecent_paths_rank ON frecent_paths(rank DESC);
        CREATE INDEX IF NOT EXISTS idx_frecent_paths_path ON frecent_paths(path);
    "#).context("Failed to create frecent_paths table")?;

    // Bootstrap from existing history: populate frecent directories from places table
    conn.execute_batch(r#"
        INSERT OR IGNORE INTO frecent_paths (path, path_type, rank, last_access, access_count)
        SELECT p.dir, 'd', COUNT(*) * 1.0, MAX(h.start_time), COUNT(*)
        FROM history h JOIN places p ON p.id = h.place_id
        GROUP BY p.dir;
    "#).context("Failed to bootstrap frecent_paths from history")?;

    info!("Migration v3: created frecent_paths table and bootstrapped from history");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fresh_database_migration() {
        let conn = Connection::open_in_memory().unwrap();

        // Should start at version 0
        assert_eq!(get_schema_version(&conn).unwrap(), 0);

        // Run migrations
        run_migrations(&conn).unwrap();

        // Should now be at current version
        assert_eq!(get_schema_version(&conn).unwrap(), CURRENT_VERSION);

        // Tables should exist
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='history'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_idempotent_migrations() {
        let conn = Connection::open_in_memory().unwrap();

        // Run migrations twice
        run_migrations(&conn).unwrap();
        run_migrations(&conn).unwrap();

        // Should still be at current version
        assert_eq!(get_schema_version(&conn).unwrap(), CURRENT_VERSION);
    }
}
