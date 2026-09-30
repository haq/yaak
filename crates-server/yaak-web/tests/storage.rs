use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use clap::Parser;
use serde_json::{Value, json};
use std::path::Path;
use tower::ServiceExt;
use yaak_web::{Config, router, try_router};

fn config(dir: Option<&Path>) -> Config {
    let mut config = Config::parse_from(["yaak-web"]);
    config.data_dir = dir.map(Into::into);
    config
}

async fn call(
    app: &Router,
    method: &str,
    path: &str,
    body: impl Into<Body>,
    content_type: &str,
) -> (StatusCode, Vec<u8>) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("content-type", content_type)
                .body(body.into())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.headers().get("cache-control").map(|v| v.to_str().unwrap()),
        Some("no-store")
    );
    let status = response.status();
    (status, to_bytes(response.into_body(), 1024 * 1024).await.unwrap().to_vec())
}

async fn rpc(app: &Router, label: &str, cmd: &str, payload: Value) -> Value {
    let (status, bytes) = call(
        app,
        "POST",
        "/v1/storage/rpc",
        json!({ "cmd": cmd, "payload": payload, "label": label }).to_string(),
        "application/json",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&bytes));
    serde_json::from_slice(&bytes).unwrap()
}

async fn models(app: &Router, label: &str, workspace: Option<&str>) -> Vec<Value> {
    let outcome =
        rpc(app, label, "models_workspace_models", json!({ "workspaceId": workspace })).await;
    serde_json::from_str(outcome["result"].as_str().unwrap()).unwrap()
}

#[tokio::test]
async fn browsers_share_models_rendering_and_bodies_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let app = router(config(Some(dir.path())));
    let workspace = json!({ "model": "workspace", "id": "wk_shared", "name": "Shared API" });
    let outcome = rpc(&app, "browser_a", "models_upsert", json!({ "model": workspace })).await;
    assert_eq!(outcome["result"], "wk_shared");
    assert_eq!(outcome["events"][0]["updateSource"]["label"], "browser_a");

    let initial = models(&app, "browser_a", Some("wk_shared")).await;
    let mut environment = initial.iter().find(|m| m["model"] == "environment").unwrap().clone();
    environment["variables"] =
        json!([{ "enabled": true, "name": "base_url", "value": "https://example.com" }]);
    rpc(&app, "browser_a", "models_upsert", json!({ "model": environment })).await;
    let cookie_jar = initial.iter().find(|m| m["model"] == "cookie_jar").unwrap().clone();

    let request = json!({ "model": "http_request", "id": "rq_shared", "workspaceId": "wk_shared",
        "name": "Shared request", "url": "${[ base_url ]}/hello" });
    rpc(&app, "browser_a", "models_upsert", json!({ "model": request })).await;
    let response = json!({ "model": "http_response", "id": "rs_shared", "workspaceId": "wk_shared",
        "requestId": "rq_shared", "state": "closed", "status": 200 });
    rpc(&app, "browser_a", "models_upsert", json!({ "model": response })).await;
    let body = b"shared response\0with binary bytes";
    assert_eq!(
        call(
            &app,
            "POST",
            "/v1/storage/blobs/rs_shared",
            body.to_vec(),
            "application/octet-stream"
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );

    // A different browser gets the server's current data, with no local import/sync.
    let other = models(&app, "browser_b", Some("wk_shared")).await;
    assert!(other.iter().any(|m| m["id"] == "rq_shared" && m["name"] == "Shared request"));
    assert!(other.iter().any(|m| m["id"] == "rs_shared"));
    drop(app);

    // New server process/router, same volume: both SQLite databases survive.
    let restarted = router(config(Some(dir.path())));
    assert!(models(&restarted, "browser_c", None).await.iter().any(|m| m["name"] == "Shared API"));
    assert_eq!(
        call(
            &restarted,
            "GET",
            "/v1/storage/blobs/rs_shared",
            Body::empty(),
            "application/octet-stream"
        )
        .await
        .1,
        body
    );
    let (status, prepared) = call(
        &restarted,
        "POST",
        "/v1/storage/prepare",
        json!({ "requestId": "rq_shared", "cookieJarId": cookie_jar["id"] }).to_string(),
        "application/json",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&prepared));
    let prepared: Value = serde_json::from_slice(&prepared).unwrap();
    assert_eq!(prepared["request"]["url"], "https://example.com/hello");
    assert_eq!(prepared["cookieJar"]["id"], cookie_jar["id"]);

    // The server delegates duplicate naming and cascaded deletion to Yaak's models.
    let duplicate = rpc(
        &restarted,
        "browser_c",
        "models_duplicate",
        json!({ "modelType": "http_request", "modelId": "rq_shared" }),
    )
    .await;
    assert_ne!(duplicate["result"], "rq_shared");
    let listed = models(&restarted, "browser_c", Some("wk_shared")).await;
    let saved_request = listed.iter().find(|m| m["id"] == "rq_shared").unwrap();
    rpc(&restarted, "browser_c", "models_delete", json!({ "model": saved_request })).await;
    let remaining = models(&restarted, "browser_c", Some("wk_shared")).await;
    assert!(!remaining.iter().any(|m| m["id"] == "rq_shared" || m["id"] == "rs_shared"));
    assert_eq!(
        call(
            &restarted,
            "DELETE",
            "/v1/storage/blobs/rs_shared",
            Body::empty(),
            "application/octet-stream"
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        call(
            &restarted,
            "GET",
            "/v1/storage/blobs/rs_shared",
            Body::empty(),
            "application/octet-stream"
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn storage_is_opt_in_and_api_errors_do_not_serve_html() {
    let assets = tempfile::tempdir().unwrap();
    std::fs::write(assets.path().join("index.html"), "<html>app</html>").unwrap();
    let mut cfg = config(None);
    cfg.serve = Some(assets.path().into());
    let app = router(cfg);
    let health = app
        .clone()
        .oneshot(Request::builder().uri("/v1/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let health: Value =
        serde_json::from_slice(&to_bytes(health.into_body(), 4096).await.unwrap()).unwrap();
    assert_eq!(health["storage"], "browser");
    let response = app
        .oneshot(
            Request::builder().uri("/v1/storage/rpc").method("POST").body(Body::empty()).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(response.headers()["content-type"], "application/json");

    let dir = tempfile::tempdir().unwrap();
    let app = router(config(Some(dir.path())));
    let (status, body) = call(
        &app,
        "POST",
        "/v1/storage/rpc",
        json!({ "cmd": "not_a_command", "payload": {}, "label": "browser" }).to_string(),
        "application/json",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        serde_json::from_slice::<Value>(&body).unwrap()["error"]
            .as_str()
            .unwrap()
            .contains("not_a_command")
    );
    assert_eq!(
        rpc(&app, "browser", "models_get_settings", json!({})).await["result"]["model"],
        "settings"
    );
}

#[tokio::test]
async fn blob_uploads_have_a_separate_size_limit() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(Some(dir.path()));
    cfg.max_request_bytes = 256;
    cfg.max_response_bytes = 1024;
    let app = router(cfg);
    assert_eq!(
        call(&app, "POST", "/v1/storage/blobs/body", vec![1; 512], "application/octet-stream")
            .await
            .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        call(&app, "POST", "/v1/storage/blobs/body", vec![2; 1025], "application/octet-stream")
            .await
            .0,
        StatusCode::PAYLOAD_TOO_LARGE
    );
    // Rejected upload leaves the last committed bytes intact.
    assert_eq!(
        call(&app, "GET", "/v1/storage/blobs/body", Body::empty(), "application/octet-stream")
            .await
            .1,
        vec![1; 512]
    );
    assert_eq!(
        call(&app, "POST", "/v1/storage/rpc", " ".repeat(257), "application/json").await.0,
        StatusCode::PAYLOAD_TOO_LARGE
    );
}

#[test]
fn invalid_data_directory_fails_startup() {
    let file = tempfile::NamedTempFile::new().unwrap();
    assert!(try_router(config(Some(file.path()))).is_err());
}
