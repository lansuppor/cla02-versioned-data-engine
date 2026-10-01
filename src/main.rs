use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::IntoResponse,
    routing::{get, put},
};
use serde::Serialize;
use serde_json::{Map, Value};
use std::{
    collections::HashMap,
    env,
    error::Error,
    sync::{RwLock, RwLockReadGuard},
};

/// 记录标识在记录对象中使用的字段名。
const ID_FIELD: &str = "id";

/// 全部数据集：数据集名 -> (记录标识 -> 记录内容)。
/// 进程内存存储；单个 RwLock 保证创建数据集与写入记录各自整体原子生效。
#[derive(Default)]
struct Store {
    datasets: RwLock<HashMap<String, HashMap<String, Value>>>,
}

#[derive(Serialize)]
struct Health {
    status: &'static str,
}

#[derive(Serialize)]
struct Version {
    name: &'static str,
    version: &'static str,
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

#[derive(Serialize)]
struct RecordResponse {
    dataset: String,
    id: String,
    record: Value,
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

fn error_response(status: StatusCode, message: impl Into<String>) -> impl IntoResponse {
    (
        status,
        Json(ErrorBody {
            error: message.into(),
        }),
    )
}

/// 数据集名称非空且不含任何空白字符。
fn valid_dataset_name(name: &str) -> bool {
    !name.is_empty() && !name.chars().any(char::is_whitespace)
}

/// 创建具名数据集。重名时返回 409，不覆盖、不清空已有数据。
async fn create_dataset(
    State(store): State<&Store>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    if !valid_dataset_name(&name) {
        return error_response(
            StatusCode::BAD_REQUEST,
            "dataset name must be non-empty and contain no whitespace",
        )
        .into_response();
    }

    let mut datasets = store.datasets.write().unwrap();
    if datasets.contains_key(&name) {
        return error_response(
            StatusCode::CONFLICT,
            format!("dataset '{name}' already exists"),
        )
        .into_response();
    }
    datasets.insert(name.clone(), HashMap::new());

    (
        StatusCode::CREATED,
        Json(serde_json::json!({ "name": name })),
    )
        .into_response()
}

/// 解析写入请求体：必须是合法 JSON、顶层为对象、含字符串类型的 `id` 字段。
/// 任何不满足都返回错误原因，调用方据此整体拒绝、不写入任何数据。
fn parse_record_body(bytes: &[u8]) -> Result<(String, Value), (StatusCode, String)> {
    let mut value: Value = serde_json::from_slice(bytes).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            format!("request body is not valid JSON: {e}"),
        )
    })?;

    let object: &mut Map<String, Value> = match value.as_object_mut() {
        Some(object) => object,
        None => {
            return Err((
                StatusCode::BAD_REQUEST,
                "record must be a JSON object containing a string 'id' field".to_owned(),
            ));
        }
    };

    let id = match object.get(ID_FIELD) {
        Some(Value::String(id)) if !id.is_empty() => id.clone(),
        Some(Value::String(_)) => {
            return Err((
                StatusCode::BAD_REQUEST,
                "record 'id' must be a non-empty string".to_owned(),
            ));
        }
        Some(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                "record 'id' field must be a string".to_owned(),
            ));
        }
        None => {
            return Err((
                StatusCode::BAD_REQUEST,
                "record is missing the required 'id' field".to_owned(),
            ));
        }
    };

    Ok((id, value))
}

/// 向数据集写入（或整体替换）一条记录。
async fn put_record(
    State(store): State<&Store>,
    Path(dataset): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    // 先在锁外完成全部解析与校验，确保失败时存储完全不被触碰。
    if !matches!(
        headers.get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok()),
        Some(content_type) if content_type
            .split(';')
            .next()
            .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case("application/json"))
    ) {
        return error_response(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "Content-Type must be application/json",
        )
        .into_response();
    }

    let (id, record) = match parse_record_body(&body) {
        Ok(parsed) => parsed,
        Err((status, message)) => return error_response(status, message).into_response(),
    };

    // 加锁后的单次 insert 即原子的整体替换：要么新记录完整可见，要么状态不变。
    let mut datasets = store.datasets.write().unwrap();
    let records = match datasets.get_mut(&dataset) {
        Some(records) => records,
        None => {
            return error_response(
                StatusCode::NOT_FOUND,
                format!("dataset '{dataset}' not found; create it before writing records"),
            )
            .into_response();
        }
    };
    let replaced = records.insert(id.clone(), record).is_some();
    drop(datasets);

    let status = if replaced {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    (
        status,
        Json(serde_json::json!({ "dataset": dataset, "id": id, "replaced": replaced })),
    )
        .into_response()
}

/// 按标识读取单条记录。数据集或标识不存在时返回 404，不创建任何占位数据。
async fn get_record(
    State(store): State<&Store>,
    Path((dataset, id)): Path<(String, String)>,
) -> impl IntoResponse {
    let datasets: RwLockReadGuard<'_, HashMap<String, HashMap<String, Value>>> =
        store.datasets.read().unwrap();
    let Some(records) = datasets.get(&dataset) else {
        return error_response(
            StatusCode::NOT_FOUND,
            format!("dataset '{dataset}' not found"),
        )
        .into_response();
    };
    match records.get(&id) {
        Some(record) => (
            StatusCode::OK,
            Json(RecordResponse {
                dataset,
                id,
                record: record.clone(),
            }),
        )
            .into_response(),
        None => error_response(
            StatusCode::NOT_FOUND,
            format!("record '{id}' not found in dataset '{dataset}'"),
        )
        .into_response(),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let bind = env::var("VDE_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_owned());
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    let store = &*Box::leak(Box::new(Store::default()));
    let app = Router::new()
        .route("/health", get(health))
        .route("/version", get(version))
        .route("/datasets/{name}", put(create_dataset))
        .route("/datasets/{name}/records", put(put_record))
        .route("/datasets/{name}/records/{id}", get(get_record))
        .with_state(store);

    println!("Listening on http://{}", listener.local_addr()?);
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
