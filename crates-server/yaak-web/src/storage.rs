//! HTTP transport for the same commands the IndexedDB worker runs.

use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::{get, post};
use serde::Deserialize;
use serde_json::{Value, json};
use std::path::Path as FilePath;
use std::sync::{Arc, Mutex};
use yaak_browser::Database;

#[derive(Clone)]
struct Storage(Arc<Mutex<Database>>);

pub fn router(
    dir: &FilePath,
    max_request_bytes: usize,
    max_blob_bytes: usize,
) -> Result<Router, String> {
    let db = Database::open(dir.join("yaak.db"), dir.join("yaak-blobs.db"))
        .map_err(|e| format!("Failed to open shared storage at {}: {e}", dir.display()))?;
    let storage = Storage(Arc::new(Mutex::new(db)));
    Ok(Router::new()
        .route("/storage/rpc", post(rpc))
        .route("/storage/prepare", post(prepare))
        .layer(DefaultBodyLimit::max(max_request_bytes))
        .merge(
            Router::new()
                .route("/storage/blobs/:id", get(blob_get).post(blob_put).delete(blob_delete))
                .layer(DefaultBodyLimit::max(max_blob_bytes)),
        )
        .with_state(storage)
        .layer(axum::middleware::from_fn(|request, next: axum::middleware::Next| async move {
            let mut response = next.run(request).await;
            response
                .headers_mut()
                .insert(header::CACHE_CONTROL, axum::http::HeaderValue::from_static("no-store"));
            response
        })))
}

impl Storage {
    // SQLite and the event receiver are synchronous. Keep them off the runtime
    // threads; serialize commands so each reply includes only its own writes.
    async fn run<T: Send + 'static>(
        &self,
        f: impl FnOnce(&Database) -> yaak_browser::Result<T> + Send + 'static,
    ) -> Result<T, StorageError> {
        let db = self.0.clone();
        tokio::task::spawn_blocking(move || {
            let db = db.lock().map_err(|_| {
                error(StatusCode::INTERNAL_SERVER_ERROR, "Shared database lock failed")
            })?;
            f(&db).map_err(|e| error(StatusCode::BAD_REQUEST, &e))
        })
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "Shared database task failed"))?
    }
}

struct StorageError {
    status: StatusCode,
    message: String,
}

impl IntoResponse for StorageError {
    fn into_response(self) -> Response {
        (self.status, Json(json!({ "error": self.message }))).into_response()
    }
}

fn error(status: StatusCode, message: &str) -> StorageError {
    StorageError { status, message: message.to_string() }
}

#[derive(Deserialize)]
struct RpcRequest {
    cmd: String,
    #[serde(default)]
    payload: Value,
    label: String,
}

async fn rpc(
    State(storage): State<Storage>,
    Json(req): Json<RpcRequest>,
) -> Result<Response, StorageError> {
    let outcome = storage.run(move |db| db.rpc(&req.cmd, req.payload, &req.label)).await?;
    Ok(Json(outcome).into_response())
}

async fn prepare(
    State(storage): State<Storage>,
    Json(payload): Json<Value>,
) -> Result<Response, StorageError> {
    let inputs = storage.run(move |db| db.prepare_http_send(payload)).await?;
    let prepared = yaak_browser::render_http_send(inputs)
        .await
        .map_err(|e| error(StatusCode::BAD_REQUEST, &e))?;
    Ok(Json(prepared).into_response())
}

async fn blob_get(
    State(storage): State<Storage>,
    Path(id): Path<String>,
) -> Result<Response, StorageError> {
    match storage.run(move |db| db.blob_get(&id)).await? {
        Some(bytes) => {
            Ok(([(header::CONTENT_TYPE, "application/octet-stream")], bytes).into_response())
        }
        None => Ok(StatusCode::NOT_FOUND.into_response()),
    }
}

async fn blob_put(
    State(storage): State<Storage>,
    Path(id): Path<String>,
    bytes: Bytes,
) -> Result<StatusCode, StorageError> {
    storage.run(move |db| db.blob_put(&id, &bytes)).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn blob_delete(
    State(storage): State<Storage>,
    Path(id): Path<String>,
) -> Result<StatusCode, StorageError> {
    storage.run(move |db| db.blob_delete(&id)).await?;
    Ok(StatusCode::NO_CONTENT)
}
