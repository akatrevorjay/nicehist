mod context;
mod db;
mod prediction;
mod protocol;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tracing::{debug, error, info, warn};

use crate::context::ContextCollector;
use crate::db::Database;
use crate::protocol::{Request, Response};

/// Get the socket path for the daemon
fn socket_path() -> PathBuf {
    if let Ok(runtime_dir) = std::env::var("XDG_RUNTIME_DIR") {
        PathBuf::from(runtime_dir).join("nicehist.sock")
    } else {
        PathBuf::from(format!("/tmp/nicehist-{}.sock", unsafe { libc::getuid() }))
    }
}

/// Get the default database path
fn db_path() -> PathBuf {
    if let Some(proj_dirs) = directories::ProjectDirs::from("", "", "nicehist") {
        let data_dir = proj_dirs.data_dir();
        std::fs::create_dir_all(data_dir).ok();
        data_dir.join("history.db")
    } else {
        let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
        PathBuf::from(home)
            .join(".local/share/nicehist")
            .join("history.db")
    }
}

async fn handle_client(stream: UnixStream, db: Database, ctx_collector: Arc<ContextCollector>) {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    loop {
        line.clear();
        match reader.read_line(&mut line).await {
            Ok(0) => break, // EOF
            Ok(_) => {
                let response = match serde_json::from_str::<Request>(&line) {
                    Ok(request) => handle_request(request, &db, &ctx_collector).await,
                    Err(e) => Response::error(-32700, format!("Parse error: {}", e)),
                };

                let response_json = serde_json::to_string(&response).unwrap_or_else(|e| {
                    serde_json::to_string(&Response::error(-32603, format!("Serialize error: {}", e)))
                        .unwrap()
                });

                if let Err(e) = writer.write_all(response_json.as_bytes()).await {
                    error!("Failed to write response: {}", e);
                    break;
                }
                if let Err(e) = writer.write_all(b"\n").await {
                    error!("Failed to write newline: {}", e);
                    break;
                }
                if let Err(e) = writer.shutdown().await {
                    debug!("Failed to shutdown writer: {}", e);
                }
                break;
            }
            Err(e) => {
                error!("Failed to read from client: {}", e);
                break;
            }
        }
    }
}

async fn handle_request(request: Request, db: &Database, ctx_collector: &ContextCollector) -> Response {
    debug!("Handling request: {:?}", request.method);

    match request.method.as_str() {
        "store" => {
            if let Some(params) = request.params {
                match serde_json::from_value::<protocol::StoreParams>(params) {
                    Ok(store_params) => {
                        match db.store_command(&store_params) {
                            Ok(id) => Response::success(request.id, serde_json::json!({"id": id})),
                            Err(e) => Response::error(-32000, format!("Store failed: {}", e)),
                        }
                    }
                    Err(e) => Response::error(-32602, format!("Invalid params: {}", e)),
                }
            } else {
                Response::error(-32602, "Missing params".to_string())
            }
        }
        "predict" => {
            if let Some(params) = request.params {
                match serde_json::from_value::<protocol::PredictParams>(params) {
                    Ok(predict_params) => {
                        match db.predict(&predict_params) {
                            Ok(suggestions) => Response::success(
                                request.id,
                                serde_json::json!({"suggestions": suggestions}),
                            ),
                            Err(e) => Response::error(-32000, format!("Predict failed: {}", e)),
                        }
                    }
                    Err(e) => Response::error(-32602, format!("Invalid params: {}", e)),
                }
            } else {
                Response::error(-32602, "Missing params".to_string())
            }
        }
        "context" => {
            if let Some(params) = request.params {
                match serde_json::from_value::<protocol::ContextParams>(params) {
                    Ok(context_params) => {
                        let ctx = ctx_collector.get_context(&context_params.cwd);
                        Response::success(request.id, serde_json::to_value(ctx).unwrap())
                    }
                    Err(e) => Response::error(-32602, format!("Invalid params: {}", e)),
                }
            } else {
                Response::error(-32602, "Missing params".to_string())
            }
        }
        "search" => {
            if let Some(params) = request.params {
                match serde_json::from_value::<protocol::SearchParams>(params) {
                    Ok(search_params) => {
                        match db.search(&search_params) {
                            Ok(results) => Response::success(
                                request.id,
                                serde_json::json!({"results": results}),
                            ),
                            Err(e) => Response::error(-32000, format!("Search failed: {}", e)),
                        }
                    }
                    Err(e) => Response::error(-32602, format!("Invalid params: {}", e)),
                }
            } else {
                Response::error(-32602, "Missing params".to_string())
            }
        }
        "delete" => {
            if let Some(params) = request.params {
                match serde_json::from_value::<protocol::DeleteParams>(params) {
                    Ok(delete_params) => {
                        match db.delete_command(&delete_params.cmd) {
                            Ok(_) => Response::success(
                                request.id,
                                serde_json::json!({"deleted": true}),
                            ),
                            Err(e) => Response::error(-32000, format!("Delete failed: {}", e)),
                        }
                    }
                    Err(e) => Response::error(-32602, format!("Invalid params: {}", e)),
                }
            } else {
                Response::error(-32602, "Missing params".to_string())
            }
        }
        "frecent_add" => {
            if let Some(params) = request.params {
                match serde_json::from_value::<protocol::FrecentAddParams>(params) {
                    Ok(frecent_params) => {
                        match db.frecent_add(&frecent_params) {
                            Ok(()) => Response::success(request.id, serde_json::json!({"ok": true})),
                            Err(e) => Response::error(-32000, format!("frecent_add failed: {}", e)),
                        }
                    }
                    Err(e) => Response::error(-32602, format!("Invalid params: {}", e)),
                }
            } else {
                Response::error(-32602, "Missing params".to_string())
            }
        }
        "frecent_query" => {
            if let Some(params) = request.params {
                match serde_json::from_value::<protocol::FrecentQueryParams>(params) {
                    Ok(query_params) => {
                        match db.frecent_query(&query_params) {
                            Ok(results) => Response::success(
                                request.id,
                                serde_json::json!({"results": results}),
                            ),
                            Err(e) => Response::error(-32000, format!("frecent_query failed: {}", e)),
                        }
                    }
                    Err(e) => Response::error(-32602, format!("Invalid params: {}", e)),
                }
            } else {
                Response::error(-32602, "Missing params".to_string())
            }
        }
        "ping" => Response::success(request.id, serde_json::json!({"pong": true})),
        _ => Response::error(-32601, format!("Method not found: {}", request.method)),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("nicehist_daemon=info".parse().unwrap()),
        )
        .init();

    let socket = socket_path();
    let db_file = db_path();

    info!("Starting nicehist daemon");
    info!("Socket: {}", socket.display());
    info!("Database: {}", db_file.display());

    // Remove existing socket if present
    if socket.exists() {
        std::fs::remove_file(&socket)?;
    }

    // Ensure parent directory exists
    if let Some(parent) = socket.parent() {
        std::fs::create_dir_all(parent).ok();
    }

    // Initialize database
    let db = Database::open(&db_file)?;
    info!("Database initialized");

    // Initialize context collector
    let ctx_collector = Arc::new(ContextCollector::new());

    // Bind to socket
    let listener = UnixListener::bind(&socket)?;
    info!("Listening on {}", socket.display());

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                debug!("New client connected");
                let db = db.clone();
                let ctx = Arc::clone(&ctx_collector);
                tokio::spawn(async move {
                    handle_client(stream, db, ctx).await;
                });
            }
            Err(e) => {
                warn!("Failed to accept connection: {}", e);
            }
        }
    }
}
