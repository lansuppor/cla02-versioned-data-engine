use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{env, error::Error, sync::Arc};

mod store;

use store::{BatchError, DiffError, LookupError, Store};

#[derive(Serialize)]
struct Health {
    status: &'static str,
}

#[derive(Serialize)]
struct Version {
    name: &'static str,
    version: &'static str,
}

async fn health() -> Json<Health> {
    Json(Health { status: "ok" })
}

async fn version() -> Json<Version> {
    Json(Version {
        name: env!("CARGO_PKG_NAME"),
        version: env!("CARGO_PKG_VERSION"),
    })
}

/// 统一错误响应：`{"error": "...", 可选的定位字段}`，不改变任何已有数据。
struct ApiError {
    status: StatusCode,
    body: Value,
}

impl ApiError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        ApiError {
            status,
            body: json!({ "error": message.into() }),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(self.body)).into_response()
    }
}

#[derive(Deserialize)]
struct WriteRequest {
    records: Vec<Value>,
    /// 是否允许用新内容替换同主键的已有记录；缺省为 false，行为与不支持替换时完全一致。
    #[serde(default)]
    replace: bool,
}

/// `POST /collections/{collection}/records`：提交一批记录（全有或全无）。
async fn write_records(
    State(s): State<Arc<Store>>,
    Path(collection): Path<String>,
    payload: Result<Json<WriteRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Json<Value>, ApiError> {
    let Json(req) = payload.map_err(|e| ApiError::new(StatusCode::BAD_REQUEST, e.body_text()))?;
    if req.records.is_empty() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "records must be a non-empty array",
        ));
    }
    match s.write_batch(&collection, &req.records, req.replace) {
        Ok(out) => Ok(Json(json!({
            "collection": out.collection,
            "accepted": out.accepted,
            "inserted": out.inserted,
            "replaced": out.replaced,
        }))),
        Err(BatchError::Rejected(r)) => Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            body: json!({
                "error": r.reason,
                "index": r.index,
                "id": r.id,
            }),
        }),
        Err(BatchError::Persist(msg)) => Err(ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, msg)),
    }
}

/// `POST /versions`：保存当前各集合数据为只读版本。
async fn save_version(State(s): State<Arc<Store>>) -> Result<Response, ApiError> {
    let v = s
        .save_version()
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, e))?;
    Ok((
        StatusCode::CREATED,
        Json(json!({ "id": v.id, "saved_at": v.saved_at })),
    )
        .into_response())
}

/// `GET /versions`：列出已保存的版本。
async fn list_versions(State(s): State<Arc<Store>>) -> Json<Value> {
    let versions: Vec<Value> = s
        .list_versions()
        .into_iter()
        .map(|v| json!({ "id": v.id, "saved_at": v.saved_at }))
        .collect();
    Json(json!({ "versions": versions }))
}

/// `GET /versions/{id}`：版本详情（含集合名列表）。
async fn get_version(
    State(s): State<Arc<Store>>,
    Path(id): Path<u64>,
) -> Result<Json<Value>, ApiError> {
    let d = s
        .get_version(id)
        .map_err(|e| lookup_error(e, "version", id))?;
    let mut collections = d.collections;
    collections.sort();
    Ok(Json(json!({
        "id": d.id,
        "saved_at": d.saved_at,
        "collections": collections,
    })))
}

/// `GET /versions/{id}/collections/{collection}/records`：查询版本内集合的全部记录。
async fn read_records(
    State(s): State<Arc<Store>>,
    Path((version_id, collection)): Path<(u64, String)>,
) -> Result<Json<Value>, ApiError> {
    let records = s
        .read_records(version_id, &collection)
        .map_err(|e| lookup_error(e, &collection, version_id))?;
    Ok(Json(json!({
        "version": version_id,
        "collection": collection,
        "records": records,
    })))
}

/// `GET /versions/{from}/collections/{collection}/diff/{to}`：
/// 比较同一集合在两个已保存版本之间的差异（只读，不生成新版本）。
async fn diff_versions(
    State(s): State<Arc<Store>>,
    Path((from, collection, to)): Path<(u64, String, u64)>,
) -> Result<Json<Value>, ApiError> {
    let d = s
        .diff_versions(from, to, &collection)
        .map_err(|e| diff_error(e, &collection, from, to))?;
    let changed: Vec<Value> = d
        .changed
        .iter()
        .map(|c| {
            json!({
                "id": c.before["id"],
                "before": c.before,
                "after": c.after,
            })
        })
        .collect();
    Ok(Json(json!({
        "from": d.from,
        "to": d.to,
        "collection": d.collection,
        "added": d.added,
        "dropped": d.dropped,
        "changed": changed,
    })))
}

/// 差异查询的错误映射：版本/集合不存在为 404，起始版本晚于目标版本为 400。
fn diff_error(e: DiffError, collection: &str, from: u64, to: u64) -> ApiError {
    match e {
        DiffError::FromVersionNotFound => {
            ApiError::new(StatusCode::NOT_FOUND, format!("version {from} not found"))
        }
        DiffError::ToVersionNotFound => {
            ApiError::new(StatusCode::NOT_FOUND, format!("version {to} not found"))
        }
        DiffError::CollectionNotFoundInFrom => ApiError::new(
            StatusCode::NOT_FOUND,
            format!("collection \"{collection}\" not found in version {from}"),
        ),
        DiffError::CollectionNotFoundInTo => ApiError::new(
            StatusCode::NOT_FOUND,
            format!("collection \"{collection}\" not found in version {to}"),
        ),
        DiffError::OutOfOrder => ApiError::new(
            StatusCode::BAD_REQUEST,
            format!("start version {from} is later than target version {to}"),
        ),
    }
}

/// 便于组合状态码与响应体。
fn lookup_error(e: LookupError, collection: &str, version: u64) -> ApiError {
    match e {
        LookupError::VersionNotFound => ApiError::new(
            StatusCode::NOT_FOUND,
            format!("version {version} not found"),
        ),
        LookupError::CollectionNotFound => ApiError::new(
            StatusCode::NOT_FOUND,
            format!("collection \"{collection}\" not found in version {version}"),
        ),
    }
}

fn app(store: Arc<Store>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/version", get(version))
        .route(
            "/collections/{collection}/records",
            axum::routing::post(write_records),
        )
        .route(
            "/versions",
            axum::routing::post(save_version).get(list_versions),
        )
        .route("/versions/{id}", get(get_version))
        .route(
            "/versions/{id}/collections/{collection}/records",
            get(read_records),
        )
        .route(
            "/versions/{from}/collections/{collection}/diff/{to}",
            get(diff_versions),
        )
        .with_state(store)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let bind = env::var("VDE_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_owned());
    let data_dir = env::var("VDE_DATA").unwrap_or_else(|_| "data".to_owned());
    let store = Arc::new(Store::open(&data_dir)?);

    let listener = tokio::net::TcpListener::bind(&bind).await?;
    println!(
        "Listening on http://{} (data dir: {})",
        listener.local_addr()?,
        data_dir
    );
    axum::serve(listener, app(store))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

#[cfg(test)]
mod http_tests;
