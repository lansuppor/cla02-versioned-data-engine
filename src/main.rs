use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use serde::{Deserialize, Serialize, de, de::Deserializer as _};
use serde_json::{Map, Value, json};
use std::cell::RefCell;
use std::rc::Rc;
use std::{env, error::Error, fmt, sync::Arc};

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
    /// 写入的记录数组；与 `delete` 互斥，缺省时按写入处理（空批次仍为 400）。
    #[serde(default)]
    records: Option<Vec<Value>>,
    /// 是否允许用新内容替换同主键的已有记录；缺省为 false，行为与不支持替换时一致。
    #[serde(default)]
    replace: bool,
    /// 删除请求：按给出的字符串主键逐条删除当前数据；非空时本请求为删除请求。
    #[serde(default)]
    delete: Option<Vec<Value>>,
}

/// 整批被业务规则拒绝时的统一响应体：`error` + `index` +（可识别时的）`id`。
fn rejection_error(r: BatchError) -> ApiError {
    match r {
        BatchError::Rejected(r) => ApiError {
            status: StatusCode::BAD_REQUEST,
            body: json!({
                "error": r.reason,
                "index": r.index,
                "id": r.id,
            }),
        },
        BatchError::Persist(msg) => ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, msg),
    }
}

/// `POST /collections/{collection}/records`：提交一批记录或一批删除（全有或全无）。
async fn write_records(
    State(s): State<Arc<Store>>,
    Path(collection): Path<String>,
    payload: Result<Json<WriteRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Json<Value>, ApiError> {
    let Json(req) = payload.map_err(|e| ApiError::new(StatusCode::BAD_REQUEST, e.body_text()))?;

    // `records` 与 `delete` 同时出现：整批拒绝，不产生任何效果。
    if req.records.is_some() && req.delete.is_some() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "\"records\" and \"delete\" must not appear in the same request",
        ));
    }

    // 删除请求：按主键逐条删除当前数据，主键缺失幂等跳过。
    if let Some(ids) = req.delete {
        if ids.is_empty() {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "delete must be a non-empty array",
            ));
        }
        let out = s.delete_batch(&collection, &ids).map_err(rejection_error)?;
        return Ok(Json(json!({
            "collection": out.collection,
            "accepted": out.accepted,
            "deleted": out.deleted,
            "missing": out.missing,
        })));
    }

    // 写入请求（缺省行为与原来完全一致）。
    let records = req.records.unwrap_or_default();
    if records.is_empty() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "records must be a non-empty array",
        ));
    }
    match s.write_batch(&collection, &records, req.replace) {
        Ok(out) => {
            // replace=false（或缺省）时响应与原来完全一致；仅替换模式附加 "replaced"。
            let mut body = json!({
                "collection": out.collection,
                "accepted": out.accepted,
                "inserted": out.inserted,
            });
            if req.replace {
                body["replaced"] = json!(out.replaced);
            }
            Ok(Json(body))
        }
        Err(e) => Err(rejection_error(e)),
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

/// `GET /versions/{id}/collections/{collection}/records/query`：
/// 按 JSON 条件对象筛选版本内集合的记录（只读，不生成新版本、不改变任何记录）。
async fn query_records(
    State(s): State<Arc<Store>>,
    Path((version_id, collection)): Path<(u64, String)>,
    body: String,
) -> Result<Json<Value>, ApiError> {
    let conditions = parse_conditions(&body).map_err(|e| match e {
        // 同一条件对象内字段名重复：整批拒绝，给出可识别的字段路径，不返回部分结果。
        CondError::Duplicate(path) => ApiError {
            status: StatusCode::BAD_REQUEST,
            body: json!({
                "error": format!("duplicate field \"{path}\" in query conditions"),
                "field": path,
            }),
        },
        CondError::Invalid(msg) => ApiError::new(StatusCode::BAD_REQUEST, msg),
    })?;
    let records = s
        .query_records(version_id, &collection, &conditions)
        .map_err(|e| lookup_error(e, &collection, version_id))?;
    Ok(Json(json!({
        "version": version_id,
        "collection": collection,
        "records": records,
    })))
}

/// 条件解析失败：同一条件对象内字段名重复（带点连接的完整路径），或 JSON 语法/形状错误。
enum CondError {
    Duplicate(String),
    Invalid(String),
}

/// 解析查询条件：请求体必须是一个 JSON 对象；任一层的条件对象内字段名重复即整批拒绝。
///
/// serde_json 默认会静默丢弃重复键，这里用自定义 Visitor 在解析期捕获重复字段，
/// 并记录点连接的完整字段路径（如 "meta.role"）。
fn parse_conditions(body: &str) -> Result<Map<String, Value>, CondError> {
    let dup: Rc<RefCell<Option<String>>> = Rc::new(RefCell::new(None));
    let mut de = serde_json::Deserializer::from_str(body);
    let result = de
        .deserialize_any(TopVisitor {
            dup: Rc::clone(&dup),
        })
        .and_then(|m| de.end().map(|_| m));
    match result {
        Ok(map) => Ok(map),
        Err(e) => {
            if let Some(path) = dup.borrow_mut().take() {
                Err(CondError::Duplicate(path))
            } else {
                Err(CondError::Invalid(format!(
                    "query conditions must be a JSON object: {e}"
                )))
            }
        }
    }
}

/// 顶层条件对象：只接受 JSON 对象。
struct TopVisitor {
    dup: Rc<RefCell<Option<String>>>,
}

impl<'de> de::Visitor<'de> for TopVisitor {
    type Value = Map<String, Value>;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a JSON object of query conditions")
    }

    fn visit_map<A: de::MapAccess<'de>>(self, access: A) -> Result<Self::Value, A::Error> {
        visit_cond_map(access, &[], &self.dup)
    }
}

/// 条件对象的公共解析：逐键读取，同层字段名重复即报错并记录完整路径；
/// 嵌套对象递归按条件对象解析（路径逐层延长），其余值按普通 JSON 值解析。
fn visit_cond_map<'de, A: de::MapAccess<'de>>(
    mut access: A,
    path: &[String],
    dup: &Rc<RefCell<Option<String>>>,
) -> Result<Map<String, Value>, A::Error> {
    let mut out = Map::new();
    while let Some(key) = access.next_key::<String>()? {
        if out.contains_key(&key) {
            let mut full = path.to_vec();
            full.push(key);
            let dotted = full.join(".");
            *dup.borrow_mut() = Some(dotted.clone());
            return Err(de::Error::custom(format!("duplicate field \"{dotted}\"")));
        }
        let mut child_path = path.to_vec();
        child_path.push(key.clone());
        let value = access.next_value_seed(CondValueSeed {
            path: child_path,
            dup: Rc::clone(dup),
        })?;
        out.insert(key, value);
    }
    Ok(out)
}

/// 条件值的解析种子：对象按子条件对象解析（继续检测重复字段），其余按普通 JSON 值。
struct CondValueSeed {
    path: Vec<String>,
    dup: Rc<RefCell<Option<String>>>,
}

impl<'de> de::DeserializeSeed<'de> for CondValueSeed {
    type Value = Value;

    fn deserialize<D: de::Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        d.deserialize_any(CondValueVisitor {
            path: self.path,
            dup: self.dup,
        })
    }
}

struct CondValueVisitor {
    path: Vec<String>,
    dup: Rc<RefCell<Option<String>>>,
}

impl<'de> de::Visitor<'de> for CondValueVisitor {
    type Value = Value;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("any JSON value")
    }

    fn visit_bool<E>(self, v: bool) -> Result<Value, E> {
        Ok(Value::Bool(v))
    }

    fn visit_i64<E>(self, v: i64) -> Result<Value, E> {
        Ok(Value::from(v))
    }

    fn visit_u64<E>(self, v: u64) -> Result<Value, E> {
        Ok(Value::from(v))
    }

    fn visit_f64<E>(self, v: f64) -> Result<Value, E> {
        Ok(Value::from(v))
    }

    fn visit_str<E>(self, v: &str) -> Result<Value, E> {
        Ok(Value::String(v.to_string()))
    }

    fn visit_string<E>(self, v: String) -> Result<Value, E> {
        Ok(Value::String(v))
    }

    fn visit_none<E>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_unit<E>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    /// 数组不是条件对象：按普通 JSON 值解析（记录中不允许数组，故永不命中）。
    fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Value, A::Error> {
        let mut items = Vec::with_capacity(seq.size_hint().unwrap_or(0));
        while let Some(item) = seq.next_element::<Value>()? {
            items.push(item);
        }
        Ok(Value::Array(items))
    }

    /// 嵌套对象即子条件：递归解析并继续检测重复字段。
    fn visit_map<A: de::MapAccess<'de>>(self, access: A) -> Result<Value, A::Error> {
        Ok(Value::Object(visit_cond_map(
            access, &self.path, &self.dup,
        )?))
    }
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
            "/versions/{id}/collections/{collection}/records/query",
            get(query_records),
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
