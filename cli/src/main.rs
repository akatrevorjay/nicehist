use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};

#[derive(Parser)]
#[command(name = "nicehist")]
#[command(about = "ZSH history with ML-based prediction")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Search command history
    Search {
        /// Search pattern
        pattern: String,
        /// Limit results
        #[arg(short, long, default_value = "20")]
        limit: usize,
        /// Filter by directory
        #[arg(short, long)]
        dir: Option<String>,
        /// Output commands only, one per line (for piping to fzf, etc.)
        #[arg(long)]
        plain: bool,
    },
    /// Store a command in history
    Store {
        /// Command string (use named arg to avoid clap treating -flags as options)
        #[arg(long)]
        cmd: String,
        /// Working directory
        #[arg(long, default_value_t = default_cwd())]
        cwd: String,
        /// Exit status of the command
        #[arg(long)]
        exit_status: Option<i64>,
        /// Duration in milliseconds
        #[arg(long)]
        duration_ms: Option<i64>,
        /// Start time (unix epoch seconds)
        #[arg(long)]
        start_time: Option<i64>,
        /// Session ID
        #[arg(long)]
        session_id: Option<i64>,
        /// Previous command (for n-gram context)
        #[arg(long)]
        prev_cmd: Option<String>,
        /// Command before previous (for n-gram context)
        #[arg(long)]
        prev2_cmd: Option<String>,
    },
    /// Get command predictions
    Predict {
        /// Prefix to predict from (named arg to avoid clap treating -flags as options)
        #[arg(long)]
        prefix: String,
        /// Working directory
        #[arg(long, default_value_t = default_cwd())]
        cwd: String,
        /// Maximum number of predictions
        #[arg(long, default_value = "5")]
        limit: usize,
        /// Last command (for n-gram context)
        #[arg(long)]
        last_cmd: Option<String>,
        /// Previous command (for n-gram context)
        #[arg(long)]
        prev_cmd: Option<String>,
        /// Socket read timeout in milliseconds
        #[arg(long, default_value = "100")]
        timeout_ms: u64,
        /// Output one command per line, no scores (for widget consumption)
        #[arg(long)]
        plain: bool,
    },
    /// Get current directory context
    Context {
        /// Working directory
        #[arg(long, default_value_t = default_cwd())]
        cwd: String,
    },
    /// Delete a command from history
    Delete {
        /// Command string to delete (use named arg to avoid clap treating -flags as options)
        #[arg(long)]
        cmd: String,
    },
    /// Shut down the daemon
    Shutdown,
    /// Show history statistics
    Stats,
    /// Import history from zsh_history file
    Import {
        /// Path to zsh_history file
        #[arg(default_value_t = default_history_path())]
        path: String,
    },
    /// Export history in zsh_history format
    Export {
        /// Maximum entries to export (0 = all)
        #[arg(short, long, default_value = "0")]
        limit: usize,
    },
    /// Benchmark RPC round-trip timing
    Bench {
        /// Number of iterations
        #[arg(short, long, default_value = "10")]
        iterations: usize,
    },
    /// Ping the daemon
    Ping,
    /// Query frecent paths (fasd-like frecency)
    Frecent {
        /// Search terms
        terms: Vec<String>,
        /// Directories only
        #[arg(short = 'd', long)]
        dirs: bool,
        /// Files only
        #[arg(short = 'f', long)]
        files: bool,
        /// Output one path per line (for piping)
        #[arg(long)]
        plain: bool,
        /// Maximum results
        #[arg(short, long, default_value = "20")]
        limit: usize,
    },
    /// Bump a path's frecency
    FrecentAdd {
        /// Path to bump
        path: String,
        /// Path type: d (directory) or f (file)
        #[arg(short = 't', long, default_value = "d")]
        path_type: String,
    },
    /// Import fasd data file
    ImportFasd {
        /// Path to fasd data file
        #[arg(default_value_t = default_fasd_path())]
        path: String,
    },
    /// Export frecent data in fasd format
    ExportFasd {
        /// Output file (default: stdout)
        #[arg(short, long)]
        output: Option<String>,
    },
}

fn default_cwd() -> String {
    std::env::current_dir()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|_| "/".to_string())
}

fn default_fasd_path() -> String {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    format!("{}/.fasd", home)
}

fn default_history_path() -> String {
    if let Ok(histfile) = std::env::var("HISTFILE") {
        return histfile;
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    format!("{}/.zsh_history", home)
}

fn socket_path() -> PathBuf {
    if let Ok(runtime_dir) = std::env::var("XDG_RUNTIME_DIR") {
        PathBuf::from(runtime_dir).join("nicehist.sock")
    } else {
        PathBuf::from(format!(
            "/tmp/nicehist-{}.sock",
            unsafe { libc::getuid() }
        ))
    }
}

#[derive(Serialize)]
struct RpcRequest {
    method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    params: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct RpcResponse {
    result: Option<serde_json::Value>,
    error: Option<RpcError>,
}

#[derive(Deserialize)]
struct RpcError {
    code: i32,
    message: String,
}

fn send_rpc(request: &RpcRequest) -> Result<serde_json::Value> {
    let socket = socket_path();

    let mut stream = UnixStream::connect(&socket)
        .with_context(|| format!("Failed to connect to daemon at {}", socket.display()))?;

    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;

    let request_json = serde_json::to_string(request)?;
    writeln!(stream, "{}", request_json)?;
    stream.flush()?;

    let mut reader = BufReader::new(stream);
    let mut response_line = String::new();
    reader.read_line(&mut response_line)?;

    let response: RpcResponse = serde_json::from_str(&response_line)?;

    if let Some(error) = response.error {
        anyhow::bail!("RPC error {}: {}", error.code, error.message);
    }

    response.result.context("No result in response")
}

fn send_rpc_with_timeout(request: &RpcRequest, timeout: Duration) -> Result<serde_json::Value> {
    let socket = socket_path();

    let mut stream = UnixStream::connect(&socket)
        .with_context(|| format!("Failed to connect to daemon at {}", socket.display()))?;

    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;

    let request_json = serde_json::to_string(request)?;
    writeln!(stream, "{}", request_json)?;
    stream.flush()?;

    let mut reader = BufReader::new(stream);
    let mut response_line = String::new();
    reader.read_line(&mut response_line)?;

    let response: RpcResponse = serde_json::from_str(&response_line)?;

    if let Some(error) = response.error {
        anyhow::bail!("RPC error {}: {}", error.code, error.message);
    }

    response.result.context("No result in response")
}

fn cmd_search(pattern: &str, limit: usize, dir: Option<&str>, plain: bool) -> Result<()> {
    let mut params = serde_json::json!({
        "pattern": pattern,
        "limit": limit,
    });

    if let Some(d) = dir {
        params["dir"] = serde_json::json!(d);
    }

    let request = RpcRequest {
        method: "search".to_string(),
        params: Some(params),
    };

    let result = send_rpc(&request)?;

    if let Some(results) = result.get("results").and_then(|r| r.as_array()) {
        if results.is_empty() && !plain {
            println!("No results found");
        } else {
            for entry in results {
                if let Some(cmd) = entry.get("cmd").and_then(|c| c.as_str()) {
                    if plain {
                        println!("{}", cmd);
                    } else {
                        let cwd = entry
                            .get("cwd")
                            .and_then(|c| c.as_str())
                            .unwrap_or("?");
                        let exit = entry
                            .get("exit_status")
                            .and_then(|e| e.as_i64());
                        let exit_str = match exit {
                            Some(0) | None => "".to_string(),
                            Some(e) => format!(" exit={}", e),
                        };
                        let score = entry
                            .get("score")
                            .and_then(|s| s.as_f64())
                            .unwrap_or(0.0);
                        println!("{} ({:.3}){} @ {}", cmd, score, exit_str, cwd);
                    }
                }
            }
        }
    }

    Ok(())
}

fn cmd_stats() -> Result<()> {
    let request = RpcRequest {
        method: "ping".to_string(),
        params: None,
    };

    match send_rpc(&request) {
        Ok(_) => {
            println!("Daemon: running");
            println!("Socket: {}", socket_path().display());
        }
        Err(e) => {
            println!("Daemon: not running ({})", e);
        }
    }

    Ok(())
}

fn cmd_import(path: &str) -> Result<()> {
    use std::fs::File;
    use std::io::BufReader as FileBufReader;

    let path = shellexpand::tilde(path);
    let file = File::open(path.as_ref())
        .with_context(|| format!("Failed to open {}", path))?;

    let reader = FileBufReader::new(file);
    let mut count = 0;
    let mut errors = 0;

    let cwd = std::env::current_dir()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|_| "/".to_string());

    println!("Importing from {}...", path);

    for line in reader.lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => {
                errors += 1;
                continue;
            }
        };

        // Skip empty lines
        if line.trim().is_empty() {
            continue;
        }

        // Parse zsh history format
        // Format: : timestamp:0;command
        // Or just: command
        let cmd = if line.starts_with(": ") {
            // Extended history format
            if let Some(pos) = line.find(';') {
                line[pos + 1..].to_string()
            } else {
                continue;
            }
        } else {
            line
        };

        if cmd.trim().is_empty() {
            continue;
        }

        // Store via RPC
        let params = serde_json::json!({
            "cmd": cmd,
            "cwd": cwd,
            "exit_status": 0,
        });

        let request = RpcRequest {
            method: "store".to_string(),
            params: Some(params),
        };

        match send_rpc(&request) {
            Ok(_) => count += 1,
            Err(_) => errors += 1,
        }

        if count % 100 == 0 {
            print!("\rImported {} commands...", count);
            std::io::stdout().flush().ok();
        }
    }

    println!("\rImported {} commands ({} errors)", count, errors);

    Ok(())
}

fn cmd_ping() -> Result<()> {
    let request = RpcRequest {
        method: "ping".to_string(),
        params: None,
    };

    let result = send_rpc(&request)?;

    if result.get("pong").is_some() {
        println!("pong");
        Ok(())
    } else {
        anyhow::bail!("Unexpected response: {:?}", result);
    }
}

fn cmd_export(limit: usize) -> Result<()> {
    let effective_limit = if limit == 0 { 100_000 } else { limit };

    let request = RpcRequest {
        method: "search".to_string(),
        params: Some(serde_json::json!({
            "pattern": "",
            "limit": effective_limit,
        })),
    };

    let result = send_rpc(&request)?;

    if let Some(results) = result.get("results").and_then(|r| r.as_array()) {
        // Collect and reverse so oldest is first (search returns newest first)
        let entries: Vec<_> = results.iter().rev().collect();
        for entry in &entries {
            let cmd = entry.get("cmd").and_then(|c| c.as_str()).unwrap_or("");
            let timestamp = entry.get("timestamp").and_then(|t| t.as_i64()).unwrap_or(0);
            let duration_ms = entry.get("duration_ms").and_then(|d| d.as_i64()).unwrap_or(0);
            let duration_secs = duration_ms / 1000;

            // Extended zsh history format: : timestamp:duration;command
            println!(": {}:{};{}", timestamp, duration_secs, cmd);
        }
        eprintln!("Exported {} entries", entries.len());
    }

    Ok(())
}

fn cmd_bench(iterations: usize) -> Result<()> {
    use std::time::Instant;

    eprintln!("Benchmarking {} iterations...\n", iterations);

    // Benchmark ping
    let mut ping_times = Vec::new();
    for _ in 0..iterations {
        let start = Instant::now();
        let request = RpcRequest {
            method: "ping".to_string(),
            params: None,
        };
        send_rpc(&request)?;
        ping_times.push(start.elapsed());
    }

    let avg_ping = ping_times.iter().sum::<Duration>() / iterations as u32;
    let min_ping = ping_times.iter().min().unwrap();
    let max_ping = ping_times.iter().max().unwrap();
    eprintln!("ping:   avg={:?}  min={:?}  max={:?}", avg_ping, min_ping, max_ping);

    // Benchmark search (empty pattern, limit 1000)
    let mut search_times = Vec::new();
    for _ in 0..iterations {
        let start = Instant::now();
        let request = RpcRequest {
            method: "search".to_string(),
            params: Some(serde_json::json!({
                "pattern": "",
                "limit": 1000,
            })),
        };
        send_rpc(&request)?;
        search_times.push(start.elapsed());
    }

    let avg_search = search_times.iter().sum::<Duration>() / iterations as u32;
    let min_search = search_times.iter().min().unwrap();
    let max_search = search_times.iter().max().unwrap();
    eprintln!("search: avg={:?}  min={:?}  max={:?}", avg_search, min_search, max_search);

    // Benchmark predict
    let mut predict_times = Vec::new();
    for _ in 0..iterations {
        let start = Instant::now();
        let request = RpcRequest {
            method: "predict".to_string(),
            params: Some(serde_json::json!({
                "prefix": "git",
                "cwd": "/tmp",
                "limit": 5,
            })),
        };
        send_rpc(&request)?;
        predict_times.push(start.elapsed());
    }

    let avg_predict = predict_times.iter().sum::<Duration>() / iterations as u32;
    let min_predict = predict_times.iter().min().unwrap();
    let max_predict = predict_times.iter().max().unwrap();
    eprintln!("predict: avg={:?}  min={:?}  max={:?}", avg_predict, min_predict, max_predict);

    Ok(())
}

fn cmd_delete(cmd: &str) -> Result<()> {
    let request = RpcRequest {
        method: "delete".to_string(),
        params: Some(serde_json::json!({ "cmd": cmd })),
    };

    send_rpc(&request)?;
    println!("Deleted: {}", cmd);
    Ok(())
}

fn cmd_store(
    cmd: &str,
    cwd: &str,
    exit_status: Option<i64>,
    duration_ms: Option<i64>,
    start_time: Option<i64>,
    session_id: Option<i64>,
    prev_cmd: Option<&str>,
    prev2_cmd: Option<&str>,
) -> Result<()> {
    let mut params = serde_json::json!({
        "cmd": cmd,
        "cwd": cwd,
    });

    if let Some(v) = exit_status {
        params["exit_status"] = serde_json::json!(v);
    }
    if let Some(v) = duration_ms {
        params["duration_ms"] = serde_json::json!(v);
    }
    if let Some(v) = start_time {
        params["start_time"] = serde_json::json!(v);
    }
    if let Some(v) = session_id {
        params["session_id"] = serde_json::json!(v);
    }
    if let Some(v) = prev_cmd {
        params["prev_cmd"] = serde_json::json!(v);
    }
    if let Some(v) = prev2_cmd {
        params["prev2_cmd"] = serde_json::json!(v);
    }

    let request = RpcRequest {
        method: "store".to_string(),
        params: Some(params),
    };

    send_rpc(&request)?;
    Ok(())
}

fn cmd_predict(
    prefix: &str,
    cwd: &str,
    limit: usize,
    last_cmd: Option<&str>,
    prev_cmd: Option<&str>,
    timeout_ms: u64,
    plain: bool,
) -> Result<()> {
    let mut params = serde_json::json!({
        "prefix": prefix,
        "cwd": cwd,
        "limit": limit,
    });

    let mut last_cmds = Vec::new();
    if let Some(c) = last_cmd {
        last_cmds.push(serde_json::json!(c));
    }
    if let Some(c) = prev_cmd {
        last_cmds.push(serde_json::json!(c));
    }
    if !last_cmds.is_empty() {
        params["last_cmds"] = serde_json::json!(last_cmds);
    }

    let request = RpcRequest {
        method: "predict".to_string(),
        params: Some(params),
    };

    let timeout = Duration::from_millis(timeout_ms);
    let result = send_rpc_with_timeout(&request, timeout)?;

    if let Some(suggestions) = result.get("suggestions").and_then(|s| s.as_array()) {
        for (i, entry) in suggestions.iter().enumerate() {
            if let Some(cmd) = entry.get("cmd").and_then(|c| c.as_str()) {
                if plain {
                    println!("{}", cmd);
                } else {
                    let score = entry
                        .get("score")
                        .and_then(|s| s.as_f64())
                        .unwrap_or(0.0);
                    println!("{}. {} ({:.3})", i + 1, cmd, score);
                }
            }
        }
    }

    Ok(())
}

fn cmd_context(cwd: &str) -> Result<()> {
    let request = RpcRequest {
        method: "context".to_string(),
        params: Some(serde_json::json!({ "cwd": cwd })),
    };

    let result = send_rpc(&request)?;

    if let Some(obj) = result.as_object() {
        for (key, value) in obj {
            if let Some(s) = value.as_str() {
                if !s.is_empty() {
                    println!("{}={}", key, s);
                }
            }
        }
    }

    Ok(())
}

fn cmd_shutdown() -> Result<()> {
    let request = RpcRequest {
        method: "shutdown".to_string(),
        params: None,
    };

    // Best effort — daemon may close the connection before responding
    let _ = send_rpc(&request);
    Ok(())
}

fn cmd_frecent(terms: &[String], path_type: Option<&str>, plain: bool, limit: usize) -> Result<()> {
    let mut params = serde_json::json!({
        "terms": terms,
        "limit": limit,
    });

    if let Some(pt) = path_type {
        params["path_type"] = serde_json::json!(pt);
    }

    let request = RpcRequest {
        method: "frecent_query".to_string(),
        params: Some(params),
    };

    let result = send_rpc(&request)?;

    if let Some(results) = result.get("results").and_then(|r| r.as_array()) {
        if results.is_empty() && !plain {
            println!("No frecent paths found");
        } else {
            for entry in results {
                if let Some(path) = entry.get("path").and_then(|p| p.as_str()) {
                    if plain {
                        println!("{}", path);
                    } else {
                        let score = entry
                            .get("score")
                            .and_then(|s| s.as_f64())
                            .unwrap_or(0.0);
                        let pt = entry
                            .get("path_type")
                            .and_then(|t| t.as_str())
                            .unwrap_or("?");
                        println!("{:.1}\t{}\t{}", score, pt, path);
                    }
                }
            }
        }
    }

    Ok(())
}

fn cmd_frecent_add(path: &str, path_type: &str) -> Result<()> {
    let request = RpcRequest {
        method: "frecent_add".to_string(),
        params: Some(serde_json::json!({
            "path": path,
            "path_type": path_type,
        })),
    };

    send_rpc(&request)?;
    Ok(())
}

fn cmd_import_fasd(path: &str) -> Result<()> {
    use std::fs::File;
    use std::io::BufReader as FileBufReader;

    let path = shellexpand::tilde(path);
    let file = File::open(path.as_ref())
        .with_context(|| format!("Failed to open fasd data file: {}", path))?;

    let reader = FileBufReader::new(file);
    let mut count = 0;
    let mut errors = 0;

    println!("Importing fasd data from {}...", path);

    for line in reader.lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => {
                errors += 1;
                continue;
            }
        };

        let line = line.trim().to_string();
        if line.is_empty() {
            continue;
        }

        // fasd format: path|rank|timestamp
        let parts: Vec<&str> = line.rsplitn(3, '|').collect();
        if parts.len() < 3 {
            errors += 1;
            continue;
        }

        // rsplitn reverses: [timestamp, rank, path]
        let timestamp: i64 = match parts[0].parse() {
            Ok(t) => t,
            Err(_) => {
                errors += 1;
                continue;
            }
        };
        let rank: f64 = match parts[1].parse() {
            Ok(r) => r,
            Err(_) => {
                errors += 1;
                continue;
            }
        };
        let entry_path = parts[2];

        // Determine type from filesystem
        let path_type = if std::path::Path::new(entry_path).is_dir() {
            "d"
        } else {
            "f"
        };

        let request = RpcRequest {
            method: "frecent_add".to_string(),
            params: Some(serde_json::json!({
                "path": entry_path,
                "path_type": path_type,
                "rank": rank,
                "timestamp": timestamp,
            })),
        };

        match send_rpc(&request) {
            Ok(_) => count += 1,
            Err(_) => errors += 1,
        }

        if count % 50 == 0 && count > 0 {
            eprint!("\rImported {} entries...", count);
            std::io::stderr().flush().ok();
        }
    }

    eprintln!("\rImported {} fasd entries ({} errors)", count, errors);

    Ok(())
}

fn cmd_export_fasd(output: Option<&str>) -> Result<()> {
    let request = RpcRequest {
        method: "frecent_query".to_string(),
        params: Some(serde_json::json!({
            "terms": [],
            "limit": 100000,
            "raw": true,
        })),
    };

    let result = send_rpc(&request)?;

    let mut writer: Box<dyn Write> = if let Some(path) = output {
        Box::new(std::fs::File::create(path)
            .with_context(|| format!("Failed to create output file: {}", path))?)
    } else {
        Box::new(std::io::stdout())
    };

    let mut count = 0;
    if let Some(results) = result.get("results").and_then(|r| r.as_array()) {
        for entry in results {
            let path = entry.get("path").and_then(|p| p.as_str()).unwrap_or("");
            let rank = entry.get("rank").and_then(|r| r.as_f64()).unwrap_or(0.0);
            let last_access = entry.get("last_access").and_then(|t| t.as_i64()).unwrap_or(0);

            if !path.is_empty() {
                writeln!(writer, "{}|{}|{}", path, rank, last_access)?;
                count += 1;
            }
        }
    }

    if output.is_some() {
        eprintln!("Exported {} entries to {}", count, output.unwrap());
    }

    Ok(())
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Search { pattern, limit, dir, plain } => {
            cmd_search(&pattern, limit, dir.as_deref(), plain)?;
        }
        Commands::Store {
            cmd, cwd, exit_status, duration_ms, start_time,
            session_id, prev_cmd, prev2_cmd,
        } => {
            cmd_store(&cmd, &cwd, exit_status, duration_ms, start_time,
                      session_id, prev_cmd.as_deref(), prev2_cmd.as_deref())?;
        }
        Commands::Predict {
            prefix, cwd, limit, last_cmd, prev_cmd, timeout_ms, plain,
        } => {
            cmd_predict(&prefix, &cwd, limit, last_cmd.as_deref(),
                        prev_cmd.as_deref(), timeout_ms, plain)?;
        }
        Commands::Context { cwd } => {
            cmd_context(&cwd)?;
        }
        Commands::Delete { cmd } => {
            cmd_delete(&cmd)?;
        }
        Commands::Shutdown => {
            cmd_shutdown()?;
        }
        Commands::Stats => {
            cmd_stats()?;
        }
        Commands::Import { path } => {
            cmd_import(&path)?;
        }
        Commands::Export { limit } => {
            cmd_export(limit)?;
        }
        Commands::Bench { iterations } => {
            cmd_bench(iterations)?;
        }
        Commands::Ping => {
            cmd_ping()?;
        }
        Commands::Frecent { terms, dirs, files, plain, limit } => {
            let path_type = if dirs {
                Some("d")
            } else if files {
                Some("f")
            } else {
                None
            };
            cmd_frecent(&terms, path_type, plain, limit)?;
        }
        Commands::FrecentAdd { path, path_type } => {
            cmd_frecent_add(&path, &path_type)?;
        }
        Commands::ImportFasd { path } => {
            cmd_import_fasd(&path)?;
        }
        Commands::ExportFasd { output } => {
            cmd_export_fasd(output.as_deref())?;
        }
    }

    Ok(())
}
