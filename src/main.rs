use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use serde::Serialize;
use serde_json::{Map, Value, json};
use std::{
    collections::HashMap,
    env,
    error::Error,
    sync::{Arc, Mutex},
};

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

/// 数据集名称 -> (记录标识 -> 记录内容)。
/// 所有变更在单个互斥锁内完成"校验 + 写入"，保证每次请求原子生效。
#[derive(Default)]
struct Store {
    datasets: HashMap<String, HashMap<String, Value>>,
}

type SharedStore = Arc<Mutex<Store>>;

fn error(status: StatusCode, message: impl Into<String>) -> Response {
    (status, Json(json!({ "error": message.into() }))).into_response()
}

/// 解析请求体为 JSON 对象；非法 JSON 或非对象一律拒绝。
fn parse_json_object(body: &Bytes) -> Result<Map<String, Value>, Response> {
    let value: Value = serde_json::from_slice(body)
        .map_err(|e| error(StatusCode::BAD_REQUEST, format!("invalid JSON body: {e}")))?;
    match value {
        Value::Object(map) => Ok(map),
        _ => Err(error(
            StatusCode::BAD_REQUEST,
            "request body must be a JSON object",
        )),
    }
}

async fn create_dataset(State(store): State<SharedStore>, body: Bytes) -> Response {
    let object = match parse_json_object(&body) {
        Ok(object) => object,
        Err(response) => return response,
    };
    let name = match object.get("name").and_then(Value::as_str) {
        Some(name) => name,
        None => {
            return error(
                StatusCode::BAD_REQUEST,
                "field \"name\" is required and must be a string",
            );
        }
    };
    if name.is_empty() || name.chars().any(char::is_whitespace) {
        return error(
            StatusCode::BAD_REQUEST,
            "dataset name must be non-empty and contain no whitespace",
        );
    }

    let mut store = store.lock().expect("store mutex poisoned");
    if store.datasets.contains_key(name) {
        return error(
            StatusCode::CONFLICT,
            format!("dataset \"{name}\" already exists"),
        );
    }
    store.datasets.insert(name.to_owned(), HashMap::new());
    (StatusCode::CREATED, Json(json!({ "name": name }))).into_response()
}

async fn write_record(
    State(store): State<SharedStore>,
    Path(dataset): Path<String>,
    body: Bytes,
) -> Response {
    let object = match parse_json_object(&body) {
        Ok(object) => object,
        Err(response) => return response,
    };
    let id = match object.get("id").and_then(Value::as_str) {
        Some(id) => id.to_owned(),
        None => {
            return error(
                StatusCode::BAD_REQUEST,
                "record must contain field \"id\" of type string",
            );
        }
    };

    let mut store = store.lock().expect("store mutex poisoned");
    let Some(records) = store.datasets.get_mut(&dataset) else {
        return error(
            StatusCode::NOT_FOUND,
            format!("dataset \"{dataset}\" not found"),
        );
    };
    // 同一标识整体替换，其余记录不受影响。
    records.insert(id.clone(), Value::Object(object));
    (
        StatusCode::OK,
        Json(json!({ "dataset": dataset, "id": id })),
    )
        .into_response()
}

async fn read_record(
    State(store): State<SharedStore>,
    Path((dataset, id)): Path<(String, String)>,
) -> Response {
    let store = store.lock().expect("store mutex poisoned");
    let Some(records) = store.datasets.get(&dataset) else {
        return error(
            StatusCode::NOT_FOUND,
            format!("dataset \"{dataset}\" not found"),
        );
    };
    let Some(record) = records.get(&id) else {
        return error(
            StatusCode::NOT_FOUND,
            format!("record \"{id}\" not found in dataset \"{dataset}\""),
        );
    };
    (
        StatusCode::OK,
        Json(json!({ "dataset": dataset, "id": id, "record": record })),
    )
        .into_response()
}

fn app(store: SharedStore) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/version", get(version))
        .route("/datasets", post(create_dataset))
        .route("/datasets/{dataset}/records", put(write_record))
        .route("/datasets/{dataset}/records/{id}", get(read_record))
        .with_state(store)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let bind = env::var("VDE_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_owned());
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    let app = app(SharedStore::default());

    println!("Listening on http://{}", listener.local_addr()?);
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> SharedStore {
        SharedStore::default()
    }

    async fn body_json(response: Response) -> Value {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body");
        serde_json::from_slice(&bytes).expect("response is JSON")
    }

    #[tokio::test]
    async fn create_dataset_then_read_write_record() {
        let store = store();

        let response = create_dataset(State(store.clone()), Bytes::from(r#"{"name":"users"}"#)).await;
        assert_eq!(response.status(), StatusCode::CREATED);

        let record = r#"{"id":"u1","name":"Ada","tags":["a","b"],"meta":{"age":36}}"#;
        let response = write_record(
            State(store.clone()),
            Path("users".to_owned()),
            Bytes::from(record),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);

        let response = read_record(
            State(store.clone()),
            Path(("users".to_owned(), "u1".to_owned())),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        assert_eq!(body["dataset"], "users");
        assert_eq!(body["id"], "u1");
        assert_eq!(body["record"], serde_json::from_str::<Value>(record).unwrap());

        // 重复读取结果一致。
        let again = read_record(
            State(store.clone()),
            Path(("users".to_owned(), "u1".to_owned())),
        )
        .await;
        assert_eq!(body_json(again).await, body);
    }

    #[tokio::test]
    async fn duplicate_dataset_keeps_existing_data() {
        let store = store();
        create_dataset(State(store.clone()), Bytes::from(r#"{"name":"d"}"#)).await;
        write_record(
            State(store.clone()),
            Path("d".to_owned()),
            Bytes::from(r#"{"id":"r1","v":1}"#),
        )
        .await;

        let response = create_dataset(State(store.clone()), Bytes::from(r#"{"name":"d"}"#)).await;
        assert_eq!(response.status(), StatusCode::CONFLICT);

        let response = read_record(
            State(store.clone()),
            Path(("d".to_owned(), "r1".to_owned())),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_json(response).await["record"]["v"], 1);
    }

    #[tokio::test]
    async fn invalid_dataset_names_rejected() {
        let store = store();
        for body in [r#"{"name":""}"#, r#"{"name":"a b"}"#, r#"{"name":"a\tb"}"#, r#"{"name":1}"#, r#"{}"#] {
            let response = create_dataset(State(store.clone()), Bytes::from(body)).await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "body: {body}");
        }
        assert!(store.lock().unwrap().datasets.is_empty());
    }

    #[tokio::test]
    async fn write_to_missing_dataset_fails_without_creating_it() {
        let store = store();
        let response = write_record(
            State(store.clone()),
            Path("ghost".to_owned()),
            Bytes::from(r#"{"id":"r1"}"#),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert!(store.lock().unwrap().datasets.is_empty());
    }

    #[tokio::test]
    async fn rewrite_replaces_whole_record_in_order() {
        let store = store();
        create_dataset(State(store.clone()), Bytes::from(r#"{"name":"d"}"#)).await;
        write_record(State(store.clone()), Path("d".to_owned()), Bytes::from(r#"{"id":"r","a":1,"b":2}"#)).await;
        write_record(State(store.clone()), Path("d".to_owned()), Bytes::from(r#"{"id":"r","a":9}"#)).await;

        let response = read_record(State(store.clone()), Path(("d".to_owned(), "r".to_owned()))).await;
        let body = body_json(response).await;
        // 整体替换：旧字段 b 消失，内容为最后一次写入。
        assert_eq!(body["record"], json!({"id": "r", "a": 9}));
    }

    #[tokio::test]
    async fn missing_record_or_dataset_returns_not_found() {
        let store = store();
        create_dataset(State(store.clone()), Bytes::from(r#"{"name":"d"}"#)).await;

        let response = read_record(State(store.clone()), Path(("d".to_owned(), "nope".to_owned()))).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let response = read_record(State(store.clone()), Path(("nope".to_owned(), "r".to_owned()))).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn malformed_record_requests_rejected_without_partial_writes() {
        let store = store();
        create_dataset(State(store.clone()), Bytes::from(r#"{"name":"d"}"#)).await;

        for body in [
            "not json",
            r#"[1,2]"#,
            r#"{"v":1}"#,          // 缺少 id
            r#"{"id":1}"#,         // id 不是字符串
            r#"{"id":null}"#,
        ] {
            let response = write_record(
                State(store.clone()),
                Path("d".to_owned()),
                Bytes::from(body),
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "body: {body}");
        }
        assert!(store.lock().unwrap().datasets["d"].is_empty());
    }
}
