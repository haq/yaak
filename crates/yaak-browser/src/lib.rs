//! Database commands shared by the IndexedDB worker and the self-hosted server.
//!
//! Host adapters only open the storage and transport these results. Model rules,
//! migrations, request rendering, and command dispatch are implemented once.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::mpsc;
use yaak_models::blob_manager::{BlobManager, BodyChunk};
use yaak_models::cookies::apply_cookie_changes;
use yaak_models::models::{
    AnyModel, Cookie, CookieJar, HttpRequest, HttpResponseEvent, HttpResponseEventData,
    HttpSendSettings,
};
use yaak_models::models_ops;
use yaak_models::query_manager::QueryManager;
use yaak_models::render::render_http_request;
use yaak_models::util::{ModelPayload, UpdateSource};
use yaak_templates::{RenderOptions, TemplateCallback};

pub type Result<T> = std::result::Result<T, String>;

fn error_message(e: impl std::fmt::Display) -> String {
    e.to_string()
}

const EXPORT_VERSION: &str = "web";

pub struct Database {
    queries: QueryManager,
    blobs: BlobManager,
    events: mpsc::Receiver<ModelPayload>,
}

impl Database {
    pub fn open(db_path: impl AsRef<Path>, blob_path: impl AsRef<Path>) -> Result<Self> {
        let (queries, blobs, events) =
            yaak_models::init_standalone(db_path, blob_path).map_err(error_message)?;
        queries
            .with_tx(|tx| yaak_lifecycle::on_launch(&yaak_lifecycle::Host::owner(), tx, &blobs))
            .map_err(error_message)?;
        // Startup changes have no calling browser and must not be attributed to its first RPC.
        let _ = events.try_iter().count();
        Ok(Self { queries, blobs, events })
    }

    pub fn rpc(&self, cmd: &str, payload: serde_json::Value, label: &str) -> Result<RpcOutcome> {
        let source = UpdateSource::from_window_label(label);
        let result = dispatch(self, cmd, payload, &source);
        let events = self.events.try_iter().collect();
        Ok(RpcOutcome { result: result?, events })
    }
}

fn from_json<T: for<'de> Deserialize<'de>>(payload: serde_json::Value) -> Result<T> {
    serde_json::from_value(payload).map_err(error_message)
}

fn to_json<T: Serialize>(value: T) -> Result<serde_json::Value> {
    serde_json::to_value(value).map_err(error_message)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcOutcome {
    pub result: serde_json::Value,
    pub events: Vec<ModelPayload>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WorkspaceModelsReq {
    workspace_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ModelReq {
    model: AnyModel,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DuplicateReq {
    model_type: String,
    model_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WorkspaceIdReq {
    workspace_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RequestIdReq {
    request_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct UpsertIntrospectionReq {
    workspace_id: String,
    request_id: String,
    content: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExportDataReq {
    workspace_ids: Vec<String>,
    include_private_environments: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResponseIdReq {
    response_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PersistSendCookiesReq {
    cookie_jar_id: String,
    before: Vec<Cookie>,
    after: Vec<Cookie>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct InsertResponseEventsReq {
    response_id: String,
    workspace_id: String,
    events: Vec<HttpResponseEventData>,
}

fn dispatch(
    host: &Database,
    cmd: &str,
    payload: serde_json::Value,
    source: &UpdateSource,
) -> Result<serde_json::Value> {
    match cmd {
        // The one big read. Same list, same order, and the same lazy
        // creates (settings, cookie jar, base environment) as
        // `models_workspace_models` on the desktop — this call is where an
        // empty database becomes a usable one. Returned as a JSON *string*
        // because that is what the desktop returns and what the store parses.
        "models_workspace_models" => {
            let req: WorkspaceModelsReq = from_json(payload)?;
            let db = host.queries.connect();
            let mut list: Vec<AnyModel> = Vec::new();

            list.push(db.get_settings().into());
            list.extend(db.list_workspaces().map_err(error_message)?.into_iter().map(Into::into));
            list.extend(db.list_key_values().map_err(error_message)?.into_iter().map(Into::into));
            // No plugin runtime to resolve these against; the rows are still
            // the truth about what is installed.
            list.extend(db.list_plugins().map_err(error_message)?.into_iter().map(Into::into));

            if let Some(wid) = req.workspace_id.as_deref() {
                let e = error_message;
                // Opening a workspace is where the rows it is assumed to have get created
                host.queries
                    .with_tx(|tx| {
                        tx.ensure_base_environment(wid)?;
                        tx.ensure_default_cookie_jar(wid)?;
                        tx.ensure_workspace_meta(wid)?;
                        Ok::<(), yaak_models::error::Error>(())
                    })
                    .map_err(e)?;
                list.extend(db.list_cookie_jars(wid).map_err(e)?.into_iter().map(Into::into));
                list.extend(db.list_environments(wid).map_err(e)?.into_iter().map(Into::into));
                list.extend(db.list_folders(wid).map_err(e)?.into_iter().map(Into::into));
                list.extend(db.list_grpc_connections(wid).map_err(e)?.into_iter().map(Into::into));
                list.extend(db.list_grpc_requests(wid).map_err(e)?.into_iter().map(Into::into));
                list.extend(db.list_http_requests(wid).map_err(e)?.into_iter().map(Into::into));
                list.extend(
                    db.list_http_responses(wid, None).map_err(e)?.into_iter().map(Into::into),
                );
                list.extend(
                    db.list_websocket_connections(wid).map_err(e)?.into_iter().map(Into::into),
                );
                list.extend(
                    db.list_websocket_requests(wid).map_err(e)?.into_iter().map(Into::into),
                );
                list.extend(db.list_workspace_metas(wid).map_err(e)?.into_iter().map(Into::into));
            }

            to_json(serde_json::to_string(&list).map_err(error_message)?)
        }

        "models_upsert" => {
            let req: ModelReq = from_json(payload)?;
            let id = host
                .queries
                .with_tx(|tx| models_ops::upsert_model(tx, &host.blobs, req.model, source))
                .map_err(error_message)?;
            to_json(id)
        }

        // Deletes and duplicates cascade, so they run in a transaction, as on
        // the desktop.
        "models_delete" => {
            let req: ModelReq = from_json(payload)?;
            let id = host
                .queries
                .with_tx(|tx| models_ops::delete_model(tx, &host.blobs, req.model, source))
                .map_err(error_message)?;
            to_json(id)
        }

        "models_duplicate" => {
            let req: DuplicateReq = from_json(payload)?;
            let id = host
                .queries
                .with_tx(|tx| {
                    models_ops::duplicate_model(tx, &req.model_type, &req.model_id, source)
                })
                .map_err(error_message)?;
            to_json(id)
        }

        "models_get_settings" => to_json(host.queries.connect().get_settings()),

        "models_get_graphql_introspection" => {
            let req: RequestIdReq = from_json(payload)?;
            to_json(host.queries.connect().get_graphql_introspection(&req.request_id))
        }

        "models_upsert_graphql_introspection" => {
            let req: UpsertIntrospectionReq = from_json(payload)?;
            let saved = host
                .queries
                .with_tx(|tx| {
                    tx.upsert_graphql_introspection(
                        &req.workspace_id,
                        &req.request_id,
                        req.content,
                        source,
                    )
                })
                .map_err(error_message)?;
            to_json(saved)
        }

        // Nothing here can open a socket, so no connection ever produced any.
        "models_grpc_events" | "models_websocket_events" => to_json(Vec::<()>::new()),

        "web_get_http_request" => {
            let req: RequestIdReq = from_json(payload)?;
            to_json(
                host.queries.connect().get_http_request(&req.request_id).map_err(error_message)?,
            )
        }

        "cmd_get_http_response_events" => {
            let req: ResponseIdReq = from_json(payload)?;
            to_json(
                host.queries
                    .connect()
                    .list_http_response_events(&req.response_id)
                    .map_err(error_message)?,
            )
        }

        // The cookies a send set or cleared, applied to the jar as it is *now* rather than
        // written over it, so an edit made while the send was in flight survives.
        "web_persist_send_cookies" => {
            let req: PersistSendCookiesReq = from_json(payload)?;
            if req.before == req.after {
                return to_json(());
            }
            host.queries
                .with_tx(|tx| {
                    let jar = tx.get_cookie_jar(&req.cookie_jar_id)?;
                    let cookies =
                        apply_cookie_changes(jar.cookies.clone(), &req.before, &req.after);
                    tx.upsert_cookie_jar(&CookieJar { cookies, ..jar }, source)
                })
                .map_err(error_message)?;
            to_json(())
        }

        // The tab's half of the send timeline: the events the proxy streamed back, recorded
        // under the response they belong to. Same rows the desktop's send task writes, and the
        // writes fan out to every tab as `model_writes` like any other.
        "web_insert_http_response_events" => {
            let req: InsertResponseEventsReq = from_json(payload)?;
            host.queries
                .with_tx(|tx| {
                    for event in req.events {
                        let model =
                            HttpResponseEvent::new(&req.response_id, &req.workspace_id, event);
                        tx.upsert_http_response_event(&model, source)?;
                    }
                    Ok::<(), yaak_models::error::Error>(())
                })
                .map_err(error_message)?;
            to_json(())
        }

        // The same `export_data` the desktop and the CLI call, not a second copy of its
        // steps. Nothing about what an export *is* is decided here; this host differs only
        // in what happens to the bytes afterwards, which is the tab's business.
        "cmd_export_data" => {
            let req: ExportDataReq = from_json(payload)?;
            to_json(
                yaak_models::export::export_data(yaak_models::export::ExportDataParams {
                    query_manager: &host.queries,
                    yaak_version: EXPORT_VERSION,
                    workspace_ids: req.workspace_ids.iter().map(|s| s.as_str()).collect(),
                    include_private_environments: req.include_private_environments,
                })
                .map_err(error_message)?,
            )
        }

        "cmd_create_example_workspace" => to_json(
            yaak_models::example::create_example_workspace(&host.queries, source)
                .map_err(error_message)?,
        ),

        "cmd_get_workspace_meta" => {
            let req: WorkspaceIdReq = from_json(payload)?;
            let workspace =
                host.queries.connect().get_workspace(&req.workspace_id).map_err(error_message)?;
            to_json(
                host.queries
                    .with_tx(|tx| tx.ensure_workspace_meta(&workspace.id))
                    .map_err(error_message)?,
            )
        }

        "cmd_delete_all_http_responses" => {
            let req: RequestIdReq = from_json(payload)?;
            host.queries
                .with_tx(|tx| tx.delete_all_http_responses_for_request(&req.request_id, source))
                .map_err(error_message)?;
            to_json(())
        }

        "cmd_delete_send_history" => {
            let req: WorkspaceIdReq = from_json(payload)?;
            host.queries
                .with_tx(|tx| {
                    tx.delete_all_http_responses_for_workspace(&req.workspace_id, source)?;
                    tx.delete_all_grpc_connections_for_workspace(&req.workspace_id, source)?;
                    tx.delete_all_websocket_connections_for_workspace(&req.workspace_id, source)?;
                    Ok::<(), yaak_models::error::Error>(())
                })
                .map_err(error_message)?;
            to_json(())
        }

        other => {
            Err(error_message(format!("yaak-web: `{other}` is not a command this host answers")))
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PrepareHttpSendReq {
    request_id: String,
    environment_id: Option<String>,
    cookie_jar_id: Option<String>,
}

/// Everything a send needs that lives in the database, resolved and rendered: the desktop's
/// `HttpSendInputs`, in the shape a tab hands to the proxy and keeps for itself.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PreparedHttpSend {
    /// The request with inherited headers and authentication applied and every template
    /// rendered. What the proxy sends, and what the response records as its request.
    request: HttpRequest,
    settings: HttpSendSettings,
    /// The `* Setting name=value` timeline lines the desktop writes at the top of a send,
    /// sources and all. The tab records them before the proxy's own events.
    setting_events: Vec<HttpResponseEventData>,
    /// The jar the send starts with, so the tab can write it back with the proxy's changes.
    cookie_jar: Option<CookieJar>,
}

/// A template callback for a host with no plugins. Variables render; a function is a clear
/// refusal naming the function, so the user knows what the request needs rather than seeing
/// an empty string sent in its place.
struct NoPluginsCallback;

impl TemplateCallback for NoPluginsCallback {
    fn run(
        &self,
        fn_name: &str,
        _args: HashMap<String, serde_json::Value>,
    ) -> impl std::future::Future<Output = yaak_templates::error::Result<String>> + Send {
        let message = format!(
            "This request uses the template function \"{fn_name}\", which needs plugins. \
             Plugins aren't available in the browser yet"
        );
        async move { Err(yaak_templates::error::Error::RenderError(message)) }
    }

    fn transform_arg(
        &self,
        _fn_name: &str,
        _arg_name: &str,
        arg_value: &str,
    ) -> yaak_templates::error::Result<String> {
        Ok(arg_value.to_string())
    }
}

/// Resolve and render a request for sending, exactly as the desktop does before it puts the
/// request on the network: the environment chain, inherited headers and auth, request
/// settings, the cookie jar. Nothing here touches a socket. What comes back is what the tab
/// posts to the Yaak server.
///
/// Refuses, with a message the user can act on, when the request needs something this host
/// doesn't have: an authentication plugin, or a template function.
pub struct SendInputs {
    request: HttpRequest,
    environment_chain: Vec<yaak_models::models::Environment>,
    settings: yaak_models::models::ResolvedHttpRequestSettings,
    cookie_jar: Option<CookieJar>,
}

impl Database {
    pub fn prepare_http_send(&self, payload: serde_json::Value) -> Result<SendInputs> {
        let req: PrepareHttpSendReq = from_json(payload)?;
        let db = self.queries.connect();
        let request = db.get_http_request(&req.request_id).map_err(error_message)?;
        let environment_chain = db
            .resolve_environments(
                &request.workspace_id,
                request.folder_id.as_deref(),
                req.environment_id.as_deref(),
            )
            .map_err(error_message)?;
        let (authentication_type, authentication, _auth_context_id) =
            db.resolve_auth_for_http_request(&request).map_err(error_message)?;
        let headers = db.resolve_headers_for_http_request(&request).map_err(error_message)?;
        let settings = db.resolve_settings_for_http_request(&request).map_err(error_message)?;
        let cookie_jar = match req.cookie_jar_id.as_deref() {
            Some(id) => Some(db.get_cookie_jar(id).map_err(error_message)?),
            None => None,
        };
        let request = HttpRequest { authentication_type, authentication, headers, ..request };
        Ok(SendInputs { request, environment_chain, settings, cookie_jar })
    }
}

pub async fn render_http_send(inputs: SendInputs) -> Result<serde_json::Value> {
    let SendInputs { request, environment_chain, settings, cookie_jar } = inputs;
    let rendered = render_http_request(
        &request,
        environment_chain,
        &NoPluginsCallback,
        &RenderOptions::throw(),
    )
    .await
    .map_err(error_message)?;

    // Authentication is applied by a plugin on the desktop. There is no plugin here, and a
    // request sent without the auth it asked for is worse than one refused with the reason.
    let auth_disabled =
        rendered.authentication.get("disabled").and_then(|v| v.as_bool()) == Some(true);
    if let Some(auth_type) = rendered.authentication_type.as_deref()
        && auth_type != "none"
        && !auth_disabled
    {
        return Err(error_message(format!(
            "This request uses {auth_type} authentication, which needs plugins. \
             Plugins aren't available in the browser yet"
        )));
    }

    let prepared = PreparedHttpSend {
        request: rendered,
        settings: HttpSendSettings::from(&settings),
        setting_events: settings.timeline_events(),
        cookie_jar,
    };
    to_json(prepared)
}

impl Database {
    /// The bytes stored under an id, or none. Ids are the desktop's: a response's
    /// own id for its body, `{responseId}.request` for the request that produced
    /// it. Bytes cross to JS as a `Uint8Array` rather than through JSON.
    pub fn blob_get(&self, id: &str) -> Result<Option<Vec<u8>>> {
        let chunks = self.blobs.connect().get_chunks(id).map_err(error_message)?;
        if chunks.is_empty() {
            return Ok(None);
        }
        Ok(Some(chunks.into_iter().flat_map(|c| c.data).collect()))
    }

    /// Store bytes under an id, replacing anything already there. Chunked the way
    /// the desktop chunks, so a body written here reads back on a desktop that
    /// imports the database, and vice versa.
    pub fn blob_put(&self, id: &str, bytes: &[u8]) -> Result<()> {
        const CHUNK: usize = 512 * 1024;
        self.blobs
            .with_tx(|b| {
                b.delete_chunks(id)?;
                for (i, part) in bytes.chunks(CHUNK).enumerate() {
                    b.insert_chunk(&BodyChunk::new(id, i as i32, part.to_vec()))?;
                }
                Ok::<(), yaak_models::error::Error>(())
            })
            .map_err(error_message)
    }

    pub fn blob_delete(&self, id: &str) -> Result<()> {
        self.blobs.with_tx(|b| b.delete_chunks(id)).map_err(error_message)
    }
}
