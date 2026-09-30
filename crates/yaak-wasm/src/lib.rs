//! IndexedDB and JavaScript adapters for the shared browser database commands.
#![cfg(target_arch = "wasm32")]

use std::cell::RefCell;
use wasm_bindgen::prelude::*;
use yaak_browser::Database;

const DB_NAME: &str = "yaak.db";
const BLOB_DB_NAME: &str = "yaak-blobs.db";
const VFS_NAME: &str = "yaak-idb";

thread_local! {
    static HOST: RefCell<Option<Database>> = const { RefCell::new(None) };
}

type Result<T> = std::result::Result<T, JsValue>;

fn js_error(e: impl std::fmt::Display) -> JsValue {
    js_sys::Error::new(&e.to_string()).into()
}

/// Register the IndexedDB-backed VFS and open the database.
///
/// Migrations run inside `init_standalone`, exactly as they do for the CLI.
/// Safe to call more than once; later calls are no-ops.
#[wasm_bindgen]
pub async fn boot() -> Result<()> {
    console_error_panic_hook::set_once();

    if HOST.with(|h| h.borrow().is_some()) {
        return Ok(());
    }

    // "Relaxed" means writes land in memory first and are flushed to
    // IndexedDB shortly after, rather than on every commit. It is the right
    // trade for a client app: a tab closing mid-flush loses at most the last
    // few writes, and the alternative (OPFS sync access handles) needs a
    // dedicated worker per file and is not available everywhere.
    let cfg = sqlite_wasm_vfs::relaxed_idb::RelaxedIdbCfgBuilder::new()
        .vfs_name(VFS_NAME)
        .preload(sqlite_wasm_vfs::relaxed_idb::Preload::All)
        .build();
    sqlite_wasm_vfs::relaxed_idb::install::<sqlite_wasm_rs::WasmOsCallback>(&cfg, true)
        .await
        .map_err(js_error)?;

    let db = Database::open(DB_NAME, BLOB_DB_NAME).map_err(js_error)?;
    HOST.with(|h| *h.borrow_mut() = Some(db));
    Ok(())
}

fn with_host<T>(f: impl FnOnce(&Database) -> yaak_browser::Result<T>) -> Result<T> {
    HOST.with(|h| {
        let h = h.borrow();
        let host = h.as_ref().ok_or_else(|| js_error("yaak-web: call boot() before rpc()"))?;
        f(host).map_err(js_error)
    })
}

fn from_js(payload: JsValue) -> Result<serde_json::Value> {
    serde_wasm_bindgen::from_value(payload).map_err(js_error)
}

fn to_js(value: impl serde::Serialize) -> Result<JsValue> {
    value.serialize(&serde_wasm_bindgen::Serializer::json_compatible()).map_err(js_error)
}

#[wasm_bindgen]
pub fn rpc(cmd: &str, payload: JsValue, label: &str) -> Result<JsValue> {
    let payload = from_js(payload)?;
    to_js(with_host(|db| db.rpc(cmd, payload, label))?)
}

#[wasm_bindgen]
pub async fn prepare_http_send(payload: JsValue) -> Result<JsValue> {
    let payload = from_js(payload)?;
    let inputs = with_host(|db| db.prepare_http_send(payload))?;
    to_js(yaak_browser::render_http_send(inputs).await.map_err(js_error)?)
}

#[wasm_bindgen]
pub fn blob_get(id: &str) -> Result<Option<Vec<u8>>> {
    with_host(|db| db.blob_get(id))
}

#[wasm_bindgen]
pub fn blob_put(id: &str, bytes: &[u8]) -> Result<()> {
    with_host(|db| db.blob_put(id, bytes))
}

#[wasm_bindgen]
pub fn blob_delete(id: &str) -> Result<()> {
    with_host(|db| db.blob_delete(id))
}
