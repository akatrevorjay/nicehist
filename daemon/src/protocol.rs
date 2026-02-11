//! JSON-RPC 2.0 protocol types for nicehist daemon communication.

use serde::{Deserialize, Serialize};

/// JSON-RPC request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    pub jsonrpc: Option<String>,
    pub id: Option<serde_json::Value>,
    pub method: String,
    #[serde(default)]
    pub params: Option<serde_json::Value>,
}

/// JSON-RPC response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

impl Response {
    pub fn success(id: Option<serde_json::Value>, result: serde_json::Value) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            id,
            result: Some(result),
            error: None,
        }
    }

    pub fn error(code: i32, message: String) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            id: None,
            result: None,
            error: Some(RpcError {
                code,
                message,
                data: None,
            }),
        }
    }
}

/// JSON-RPC error object
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

/// Parameters for the "store" method
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreParams {
    /// The command that was executed
    pub cmd: String,
    /// Current working directory
    pub cwd: String,
    /// Exit status of the command (0 = success)
    #[serde(default)]
    pub exit_status: Option<i32>,
    /// Duration in milliseconds
    #[serde(default)]
    pub duration_ms: Option<i64>,
    /// Unix timestamp when command started
    #[serde(default)]
    pub start_time: Option<i64>,
    /// Session ID (shell PID)
    #[serde(default)]
    pub session_id: Option<i64>,
    /// Previous command (for n-gram updates)
    #[serde(default)]
    pub prev_cmd: Option<String>,
    /// Second previous command (for trigram updates)
    #[serde(default)]
    pub prev2_cmd: Option<String>,
}

/// Configurable ranking weights for prediction scoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RankingWeights {
    /// Weight for command frequency score (default: 0.35)
    #[serde(default = "default_freq_weight")]
    pub frequency: f64,
    /// Weight for recency score (default: 0.30)
    #[serde(default = "default_recency_weight")]
    pub recency: f64,
    /// Score for exact directory match (default: 0.35)
    #[serde(default = "default_dir_exact_weight")]
    pub dir_exact: f64,
    /// Weight for parent directory hierarchy match (default: 0.15)
    #[serde(default = "default_dir_hierarchy_weight")]
    pub dir_hierarchy: f64,
    /// Failure penalty factor: 100% fail rate → (1 - factor)x score (default: 0.5)
    #[serde(default = "default_failure_penalty")]
    pub failure_penalty: f64,
    /// Maximum frecent directory boost (default: 0.1)
    #[serde(default = "default_frecent_boost_max")]
    pub frecent_boost_max: f64,
    /// Weight for n-gram (bigram/trigram) sequence bonus (default: 0.40)
    #[serde(default = "default_ngram_weight")]
    pub ngram: f64,
}

impl Default for RankingWeights {
    fn default() -> Self {
        Self {
            frequency: 0.35,
            recency: 0.30,
            dir_exact: 0.35,
            dir_hierarchy: 0.15,
            failure_penalty: 0.5,
            frecent_boost_max: 0.1,
            ngram: 0.40,
        }
    }
}

fn default_freq_weight() -> f64 { 0.35 }
fn default_recency_weight() -> f64 { 0.30 }
fn default_dir_exact_weight() -> f64 { 0.35 }
fn default_dir_hierarchy_weight() -> f64 { 0.15 }
fn default_failure_penalty() -> f64 { 0.5 }
fn default_frecent_boost_max() -> f64 { 0.1 }
fn default_ngram_weight() -> f64 { 0.40 }

/// Parameters for the "predict" method
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PredictParams {
    /// The current input prefix to complete
    pub prefix: String,
    /// Current working directory
    pub cwd: String,
    /// Recent commands for context (most recent first)
    #[serde(default)]
    pub last_cmds: Vec<String>,
    /// Maximum number of suggestions to return
    #[serde(default = "default_limit")]
    pub limit: usize,
    /// Enable frecent directory boost (cross-pollination)
    #[serde(default = "default_true")]
    pub frecent_boost: bool,
    /// Optional ranking weight overrides
    #[serde(default)]
    pub weights: Option<RankingWeights>,
}

fn default_true() -> bool {
    true
}

fn default_limit() -> usize {
    5
}

/// A single prediction suggestion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Suggestion {
    /// The suggested command
    pub cmd: String,
    /// Confidence score (0.0 to 1.0)
    pub score: f64,
}

/// Parameters for the "context" method
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextParams {
    /// Directory to get context for
    pub cwd: String,
}

/// Context information for a directory
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextInfo {
    /// VCS type (git, hg, or null)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vcs: Option<String>,
    /// VCS branch name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    /// VCS repository root
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vcs_root: Option<String>,
    /// Detected project type (rust, node, python, etc.)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
}

/// Parameters for the "delete" method
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteParams {
    /// The command string to delete
    pub cmd: String,
}

/// Parameters for the "search" method
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchParams {
    /// Search pattern (substring match)
    pub pattern: String,
    /// Maximum results to return
    #[serde(default = "default_search_limit")]
    pub limit: usize,
    /// Filter by directory (optional)
    #[serde(default)]
    pub dir: Option<String>,
    /// Filter by exit status (optional, 0 = success only)
    #[serde(default)]
    pub exit_status: Option<i32>,
    /// Recent commands for n-gram context scoring (most recent first)
    #[serde(default)]
    pub last_cmds: Vec<String>,
    /// Current working directory for directory affinity scoring
    #[serde(default)]
    pub cwd: Option<String>,
    /// Enable n-gram context boost in scoring (default: false for backward compat)
    #[serde(default)]
    pub ngram_boost: bool,
}

fn default_search_limit() -> usize {
    20
}

/// A search result entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    /// The command
    pub cmd: String,
    /// Directory where it was run
    pub cwd: String,
    /// When it was run (Unix timestamp)
    pub timestamp: i64,
    /// Exit status
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_status: Option<i32>,
    /// Duration in milliseconds
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<i64>,
    /// Relevance score (0.0 to 1.0) based on recency and exit status
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<f64>,
}

/// Parameters for the "frecent_add" method
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrecentAddParams {
    /// The path to add/bump
    pub path: String,
    /// Path type: "d" = directory, "f" = file
    #[serde(default = "default_path_type")]
    pub path_type: String,
    /// Override rank (for imports)
    #[serde(default)]
    pub rank: Option<f64>,
    /// Override timestamp (for imports)
    #[serde(default)]
    pub timestamp: Option<i64>,
}

fn default_path_type() -> String {
    "d".to_string()
}

/// Parameters for the "frecent_query" method
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrecentQueryParams {
    /// Search terms
    #[serde(default)]
    pub terms: Vec<String>,
    /// Filter by path type: None = any, "d" = dirs, "f" = files
    #[serde(default)]
    pub path_type: Option<String>,
    /// Maximum results to return
    #[serde(default = "default_frecent_limit")]
    pub limit: usize,
    /// Include raw rank/last_access in results (for export)
    #[serde(default)]
    pub raw: bool,
}

fn default_frecent_limit() -> usize {
    20
}

/// A frecency result entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrecencyResult {
    /// The path
    pub path: String,
    /// Path type ("d" or "f")
    pub path_type: String,
    /// Frecency score
    pub score: f64,
    /// Raw rank value (for export)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rank: Option<f64>,
    /// Last access timestamp (for export)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_access: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_parse() {
        let json = r#"{"method": "store", "params": {"cmd": "git status", "cwd": "/home/user"}}"#;
        let req: Request = serde_json::from_str(json).unwrap();
        assert_eq!(req.method, "store");
        assert!(req.params.is_some());
    }

    #[test]
    fn test_store_params_parse() {
        let json = r#"{"cmd": "git commit -m 'test'", "cwd": "/home/user/project", "exit_status": 0, "duration_ms": 1234}"#;
        let params: StoreParams = serde_json::from_str(json).unwrap();
        assert_eq!(params.cmd, "git commit -m 'test'");
        assert_eq!(params.cwd, "/home/user/project");
        assert_eq!(params.exit_status, Some(0));
        assert_eq!(params.duration_ms, Some(1234));
    }

    #[test]
    fn test_predict_params_defaults() {
        let json = r#"{"prefix": "git c", "cwd": "/home/user"}"#;
        let params: PredictParams = serde_json::from_str(json).unwrap();
        assert_eq!(params.prefix, "git c");
        assert_eq!(params.limit, 5); // default
        assert!(params.last_cmds.is_empty()); // default
    }

    #[test]
    fn test_response_success() {
        let resp = Response::success(Some(serde_json::json!(1)), serde_json::json!({"id": 42}));
        assert_eq!(resp.jsonrpc, "2.0");
        assert!(resp.result.is_some());
        assert!(resp.error.is_none());
    }

    #[test]
    fn test_response_error() {
        let resp = Response::error(-32600, "Invalid Request".to_string());
        assert_eq!(resp.jsonrpc, "2.0");
        assert!(resp.result.is_none());
        assert!(resp.error.is_some());
        assert_eq!(resp.error.unwrap().code, -32600);
    }

    #[test]
    fn test_suggestion_serialize() {
        let suggestion = Suggestion {
            cmd: "git commit".to_string(),
            score: 0.85,
        };
        let json = serde_json::to_string(&suggestion).unwrap();
        assert!(json.contains("git commit"));
        assert!(json.contains("0.85"));
    }
}
