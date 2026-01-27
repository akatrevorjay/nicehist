//! N-gram model for command prediction.
//!
//! Uses bigram (P(cmd|prev)) and trigram (P(cmd|prev2,prev1)) probabilities
//! with backoff smoothing.

use anyhow::Result;
use rusqlite::Connection;

/// N-gram statistics for a command
#[derive(Debug, Clone)]
pub struct NgramStats {
    pub command: String,
    pub frequency: i64,
    pub last_used: i64,
}

/// N-gram model for prediction
pub struct NgramModel;

impl NgramModel {
    /// Get bigram predictions: commands that often follow prev_cmd
    ///
    /// Returns commands sorted by frequency descending.
    pub fn get_bigram_predictions(
        conn: &Connection,
        prev_cmd: &str,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<NgramStats>> {
        let mut stmt = conn.prepare_cached(
            "SELECT c.argv, n.frequency, n.last_used
             FROM ngrams_2 n
             JOIN commands c ON c.id = n.command_id
             JOIN commands prev ON prev.id = n.prev_command_id
             WHERE prev.argv = ?1 AND c.argv LIKE ?2 || '%'
             ORDER BY n.frequency DESC
             LIMIT ?3",
        )?;

        let rows = stmt.query_map(
            rusqlite::params![prev_cmd, prefix, limit],
            |row| {
                Ok(NgramStats {
                    command: row.get(0)?,
                    frequency: row.get(1)?,
                    last_used: row.get(2)?,
                })
            },
        )?;

        Ok(rows.filter_map(|r| r.ok()).collect())
    }

    /// Get trigram predictions: commands that often follow (prev2, prev1)
    pub fn get_trigram_predictions(
        conn: &Connection,
        prev2_cmd: &str,
        prev1_cmd: &str,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<NgramStats>> {
        let mut stmt = conn.prepare_cached(
            "SELECT c.argv, n.frequency, n.last_used
             FROM ngrams_3 n
             JOIN commands c ON c.id = n.command_id
             JOIN commands prev1 ON prev1.id = n.prev1_command_id
             JOIN commands prev2 ON prev2.id = n.prev2_command_id
             WHERE prev2.argv = ?1 AND prev1.argv = ?2 AND c.argv LIKE ?3 || '%'
             ORDER BY n.frequency DESC
             LIMIT ?4",
        )?;

        let rows = stmt.query_map(
            rusqlite::params![prev2_cmd, prev1_cmd, prefix, limit],
            |row| {
                Ok(NgramStats {
                    command: row.get(0)?,
                    frequency: row.get(1)?,
                    last_used: row.get(2)?,
                })
            },
        )?;

        Ok(rows.filter_map(|r| r.ok()).collect())
    }

    /// Calculate backoff probability combining trigram, bigram, and unigram
    ///
    /// The presence of higher-order n-gram data (bigram, trigram) indicates
    /// stronger contextual relevance and should boost the score.
    pub fn backoff_score(
        trigram_freq: Option<i64>,
        bigram_freq: Option<i64>,
        unigram_freq: i64,
        total_commands: i64,
    ) -> f64 {
        let total = total_commands.max(1) as f64;

        // Base score from unigram frequency
        let p_unigram = unigram_freq as f64 / total;
        let base_score = (p_unigram * 100.0 + 1.0).ln() / 5.0; // Log scale, 0-1 range

        // Boost from bigram context
        let bigram_boost = if let Some(freq) = bigram_freq {
            let p_bigram = freq as f64 / unigram_freq.max(1) as f64;
            0.2 * p_bigram.min(1.0)
        } else {
            0.0
        };

        // Boost from trigram context
        let trigram_boost = if let Some(freq) = trigram_freq {
            let p_trigram = freq as f64 / bigram_freq.unwrap_or(1).max(1) as f64;
            0.15 * p_trigram.min(1.0)
        } else {
            0.0
        };

        (base_score + bigram_boost + trigram_boost).min(1.0)
    }

    /// Get unigram frequency for a command
    pub fn get_unigram_frequency(conn: &Connection, cmd: &str) -> Result<i64> {
        let freq: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM history h
                 JOIN commands c ON c.id = h.command_id
                 WHERE c.argv = ?1",
                [cmd],
                |row| row.get(0),
            )
            .unwrap_or(0);

        Ok(freq)
    }

    /// Get total command count for normalization
    pub fn get_total_commands(conn: &Connection) -> Result<i64> {
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM history", [], |row| row.get(0))
            .unwrap_or(1);

        Ok(count.max(1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backoff_score_unigram_only() {
        let score = NgramModel::backoff_score(None, None, 10, 100);
        assert!(score > 0.0);
        assert!(score < 1.0);
    }

    #[test]
    fn test_backoff_score_with_bigram() {
        let score_no_bigram = NgramModel::backoff_score(None, None, 10, 100);
        let score_with_bigram = NgramModel::backoff_score(None, Some(5), 10, 100);

        // Bigram should increase score
        assert!(score_with_bigram > score_no_bigram);
    }

    #[test]
    fn test_backoff_score_with_trigram() {
        let score_bigram = NgramModel::backoff_score(None, Some(5), 10, 100);
        let score_trigram = NgramModel::backoff_score(Some(3), Some(5), 10, 100);

        // Trigram should further increase score
        assert!(score_trigram > score_bigram);
    }

    #[test]
    fn test_backoff_score_bounds() {
        // Test edge cases
        let score_zero = NgramModel::backoff_score(None, None, 0, 100);
        let score_high = NgramModel::backoff_score(Some(100), Some(100), 100, 100);

        assert!(score_zero >= 0.0);
        assert!(score_high <= 1.0);
    }
}
