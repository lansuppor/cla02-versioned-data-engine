use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use serde::{Deserialize, Serialize, de};
use serde_json::{Value, json};
use std::{cell::RefCell, env, error::Error, fmt, rc::Rc, sync::Arc};

mod store;

use store::{BatchError, CondObject, CondValue, DiffError, LookupError, Store};

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

/// 查询请求体的自定义解析种子：逐层构建条件树。
///
/// serde_json 反序列化到 `Map` 时会静默覆盖重复键，无法事后检测，因此这里用
/// 自定义 Visitor 在解析过程中发现同一条件对象内的重复字段名并拒绝整个请求。
struct CondValueSeed {
    /// 当前值在条件树中的完整字段路径（顶层为空），用于重复字段的报错定位。
    path: String,
    /// 发现重复字段时写入其完整路径，供解析失败后构造结构化错误响应。
    dup: Rc<RefCell<Option<String>>>,
}

impl<'de> de::DeserializeSeed<'de> for CondValueSeed {
    type Value = CondValue;

    fn deserialize<D>(self, deserializer: D) -> Result<CondValue, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }
}

impl<'de> de::Visitor<'de> for CondValueSeed {
    type Value = CondValue;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a query condition value")
    }

    /// 嵌套对象表示子条件：逐键解析，同一对象内字段名重复即报错。
    fn visit_map<A>(self, mut map: A) -> Result<CondValue, A::Error>
    where
        A: de::MapAccess<'de>,
    {
        let mut entries = Vec::new();
        let mut seen = std::collections::HashSet::new();
        while let Some(key) = map.next_key::<String>()? {
            let child_path = if self.path.is_empty() {
                key.clone()
            } else {
                format!("{}.{}", self.path, key)
            };
            if !seen.insert(key.clone()) {
                *self.dup.borrow_mut() = Some(child_path.clone());
                return Err(de::Error::custom(format!(
                    "duplicate field \"{child_path}\" in query conditions"
                )));
            }
            let value = map.next_value_seed(CondValueSeed {
                path: child_path,
                dup: self.dup.clone(),
            })?;
            entries.push((key, value));
        }
        Ok(CondValue::Sub(CondObject(entries)))
    }

    // 标量期望值：字符串、整数、布尔、null。浮点数与数组在记录中不允许出现，
    // 作为期望值时按完全相等处理（必然不命中任何记录）。
    fn visit_bool<E>(self, v: bool) -> Result<CondValue, E> {
        Ok(CondValue::Scalar(Value::Bool(v)))
    }

    fn visit_i64<E>(self, v: i64) -> Result<CondValue, E> {
        Ok(CondValue::Scalar(Value::from(v)))
    }

    fn visit_u64<E>(self, v: u64) -> Result<CondValue, E> {
        Ok(CondValue::Scalar(Value::from(v)))
    }

    fn visit_f64<E>(self, v: f64) -> Result<CondValue, E>
    where
        E: de::Error,
    {
        match serde_json::Number::from_f64(v) {
            Some(n) => Ok(CondValue::Scalar(Value::Number(n))),
            None => Err(E::custom("invalid number in query conditions")),
        }
    }

    fn visit_str<E>(self, v: &str) -> Result<CondValue, E> {
        Ok(CondValue::Scalar(Value::String(v.to_owned())))
    }

    fn visit_string<E>(self, v: String) -> Result<CondValue, E> {
        Ok(CondValue::Scalar(Value::String(v)))
    }

    fn visit_unit<E>(self) -> Result<CondValue, E> {
        Ok(CondValue::Scalar(Value::Null))
    }

    fn visit_none<E>(self) -> Result<CondValue, E> {
        Ok(CondValue::Scalar(Value::Null))
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<CondValue, A::Error>
    where
        A: de::SeqAccess<'de>,
    {
        let mut items = Vec::new();
        while let Some(item) = seq.next_element::<Value>()? {
            items.push(item);
        }
        Ok(CondValue::Scalar(Value::Array(items)))
    }
}

/// 解析查询请求体为条件树。空请求体按空条件（不筛选）处理；
/// 同一条件对象内字段名重复时整批拒绝（400），并给出可识别的字段路径。
fn parse_conditions(body: &str) -> Result<CondObject, ApiError> {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return Ok(CondObject::default());
    }
    let dup = Rc::new(RefCell::new(None));
    let seed = CondValueSeed {
        path: String::new(),
        dup: dup.clone(),
    };
    let mut de = serde_json::Deserializer::from_str(trimmed);
    let parsed = de::DeserializeSeed::deserialize(seed, &mut de).and_then(|v| de.end().map(|_| v));
    match parsed {
        Ok(CondValue::Sub(conditions)) => Ok(conditions),
        Ok(_) => Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "query conditions must be a JSON object",
        )),
        Err(e) => {
            if let Some(path) = dup.borrow_mut().take() {
                Err(ApiError {
                    status: StatusCode::BAD_REQUEST,
                    body: json!({
                        "error": format!("duplicate field \"{path}\" in query conditions"),
                        "field": path,
                    }),
                })
            } else {
                Err(ApiError::new(StatusCode::BAD_REQUEST, e.to_string()))
            }
        }
    }
}

/// `GET /versions/{id}/collections/{collection}/records/query`：
/// 按条件筛选版本内集合的记录（只读，不生成新版本、不改变任何记录）。
async fn query_records(
    State(s): State<Arc<Store>>,
    Path((version_id, collection)): Path<(u64, String)>,
    body: String,
) -> Result<Json<Value>, ApiError> {
    let conditions = parse_conditions(&body)?;
    let records = s
        .query_records(version_id, &collection, &conditions)
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
