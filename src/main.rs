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

/// 一个数据集的全部状态：
/// - `records`：当前最新记录（记录标识 -> 记录内容）。
/// - `versions`：已保存版本（版本标识 -> 保存那一刻的记录快照），快照不可变。
/// - `version_seq`：数据集内自增序号，保证版本标识唯一且每次保存各自独立。
#[derive(Default)]
struct Dataset {
    records: HashMap<String, Value>,
    versions: HashMap<String, HashMap<String, Value>>,
    version_seq: u64,
}

/// 数据集名称 -> 数据集状态。
/// 所有变更在单个互斥锁内完成"校验 + 写入"，保证每次请求原子生效。
#[derive(Default)]
struct Store {
    datasets: HashMap<String, Dataset>,
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
    store.datasets.insert(name.to_owned(), Dataset::default());
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
    let Some(ds) = store.datasets.get_mut(&dataset) else {
        return error(
            StatusCode::NOT_FOUND,
            format!("dataset \"{dataset}\" not found"),
        );
    };
    // 同一标识整体替换，其余记录不受影响；已保存版本的快照不被触碰。
    ds.records.insert(id.clone(), Value::Object(object));
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
    let Some(ds) = store.datasets.get(&dataset) else {
        return error(
            StatusCode::NOT_FOUND,
            format!("dataset \"{dataset}\" not found"),
        );
    };
    let Some(record) = ds.records.get(&id) else {
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

async fn delete_record(
    State(store): State<SharedStore>,
    Path((dataset, id)): Path<(String, String)>,
) -> Response {
    let mut store = store.lock().expect("store mutex poisoned");
    let Some(ds) = store.datasets.get_mut(&dataset) else {
        return error(
            StatusCode::NOT_FOUND,
            format!("dataset \"{dataset}\" not found"),
        );
    };
    // 只从当前数据删除；版本快照中保留保存时的副本。
    if ds.records.remove(&id).is_none() {
        return error(
            StatusCode::NOT_FOUND,
            format!("record \"{id}\" not found in dataset \"{dataset}\""),
        );
    }
    (
        StatusCode::OK,
        Json(json!({ "dataset": dataset, "id": id })),
    )
        .into_response()
}

/// 为数据集当前全部记录保存一次不可变快照。
async fn save_version(
    State(store): State<SharedStore>,
    Path(dataset): Path<String>,
) -> Response {
    let mut store = store.lock().expect("store mutex poisoned");
    let Some(ds) = store.datasets.get_mut(&dataset) else {
        return error(
            StatusCode::NOT_FOUND,
            format!("dataset \"{dataset}\" not found"),
        );
    };
    // 锁内完成序号分配 + 全量深拷贝 + 插入：要么整体成功可查，要么不发生。
    ds.version_seq += 1;
    let version = format!("v{}", ds.version_seq);
    let snapshot: HashMap<String, Value> = ds
        .records
        .iter()
        .map(|(id, record)| (id.clone(), record.clone()))
        .collect();
    ds.versions.insert(version.clone(), snapshot);
    (
        StatusCode::CREATED,
        Json(json!({ "dataset": dataset, "version": version })),
    )
        .into_response()
}

/// 按版本读取单条记录。
async fn read_version_record(
    State(store): State<SharedStore>,
    Path((dataset, version, id)): Path<(String, String, String)>,
) -> Response {
    let store = store.lock().expect("store mutex poisoned");
    let Some(ds) = store.datasets.get(&dataset) else {
        return error(
            StatusCode::NOT_FOUND,
            format!("dataset \"{dataset}\" not found"),
        );
    };
    let Some(snapshot) = ds.versions.get(&version) else {
        return error(
            StatusCode::NOT_FOUND,
            format!("version \"{version}\" not found in dataset \"{dataset}\""),
        );
    };
    let Some(record) = snapshot.get(&id) else {
        return error(
            StatusCode::NOT_FOUND,
            format!(
                "record \"{id}\" not found in version \"{version}\" of dataset \"{dataset}\""
            ),
        );
    };
    (
        StatusCode::OK,
        Json(json!({
            "dataset": dataset,
            "version": version,
            "id": id,
            "record": record,
        })),
    )
        .into_response()
}

/// 按版本读取整批记录。
async fn read_version_records(
    State(store): State<SharedStore>,
    Path((dataset, version)): Path<(String, String)>,
) -> Response {
    let store = store.lock().expect("store mutex poisoned");
    let Some(ds) = store.datasets.get(&dataset) else {
        return error(
            StatusCode::NOT_FOUND,
            format!("dataset \"{dataset}\" not found"),
        );
    };
    let Some(snapshot) = ds.versions.get(&version) else {
        return error(
            StatusCode::NOT_FOUND,
            format!("version \"{version}\" not found in dataset \"{dataset}\""),
        );
    };
    (
        StatusCode::OK,
        Json(json!({
            "dataset": dataset,
            "version": version,
            "records": snapshot,
        })),
    )
        .into_response()
}

/// 比较同一数据集的两个版本，请求体 {"from":"...","to":"..."}。
async fn compare_versions(
    State(store): State<SharedStore>,
    Path(dataset): Path<String>,
    body: Bytes,
) -> Response {
    let object = match parse_json_object(&body) {
        Ok(object) => object,
        Err(response) => return response,
    };
    let from = match object.get("from").and_then(Value::as_str) {
        Some(from) => from.to_owned(),
        None => {
            return error(
                StatusCode::BAD_REQUEST,
                "field \"from\" is required and must be a string",
            );
        }
    };
    let to = match object.get("to").and_then(Value::as_str) {
        Some(to) => to.to_owned(),
        None => {
            return error(
                StatusCode::BAD_REQUEST,
                "field \"to\" is required and must be a string",
            );
        }
    };

    let store = store.lock().expect("store mutex poisoned");
    let Some(ds) = store.datasets.get(&dataset) else {
        return error(
            StatusCode::NOT_FOUND,
            format!("dataset \"{dataset}\" not found"),
        );
    };
    // 任一版本不存在则整体失败，不产生任何部分比较结果。
    let Some(from_snapshot) = ds.versions.get(&from) else {
        return error(
            StatusCode::NOT_FOUND,
            format!("version \"{from}\" not found in dataset \"{dataset}\""),
        );
    };
    let Some(to_snapshot) = ds.versions.get(&to) else {
        return error(
            StatusCode::NOT_FOUND,
            format!("version \"{to}\" not found in dataset \"{dataset}\""),
        );
    };

    // 按记录标识排序，保证比较结果稳定可核对。
    let mut ids: Vec<&String> = from_snapshot.keys().chain(to_snapshot.keys()).collect();
    ids.sort_unstable();
    ids.dedup();

    let mut added = Vec::new();
    let mut removed = Vec::new();
    let mut changed = Vec::new();
    for id in ids {
        match (from_snapshot.get(id), to_snapshot.get(id)) {
            (None, Some(after)) => added.push(json!({ "id": id, "record": after })),
            (Some(before), None) => removed.push(json!({ "id": id, "record": before })),
            (Some(before), Some(after)) if before != after => {
                changed.push(json!({ "id": id, "before": before, "after": after }));
            }
            _ => {}
        }
    }

    (
        StatusCode::OK,
        Json(json!({
            "dataset": dataset,
            "from": from,
            "to": to,
            "added": added,
            "removed": removed,
            "changed": changed,
        })),
    )
        .into_response()
}

fn app(store: SharedStore) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/version", get(version))
        .route("/datasets", post(create_dataset))
        .route("/datasets/{dataset}/records", put(write_record))
        .route("/datasets/{dataset}/records/{id}", get(read_record).delete(delete_record))
        .route("/datasets/{dataset}/versions", post(save_version))
        .route(
            "/datasets/{dataset}/versions/{version}/records",
            get(read_version_records),
        )
        .route(
            "/datasets/{dataset}/versions/{version}/records/{id}",
            get(read_version_record),
        )
        .route(
            "/datasets/{dataset}/versions/compare",
            post(compare_versions),
        )
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

    async fn create(store: &SharedStore, name: &str) {
        let response =
            create_dataset(State(store.clone()), Bytes::from(format!(r#"{{"name":"{name}"}}"#)))
                .await;
        assert_eq!(response.status(), StatusCode::CREATED);
    }

    async fn put(store: &SharedStore, dataset: &str, record: &str) {
        let response = write_record(
            State(store.clone()),
            Path(dataset.to_owned()),
            Bytes::copy_from_slice(record.as_bytes()),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    async fn save(store: &SharedStore, dataset: &str) -> String {
        let response = save_version(State(store.clone()), Path(dataset.to_owned())).await;
        assert_eq!(response.status(), StatusCode::CREATED);
        body_json(response).await["version"].as_str().unwrap().to_owned()
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
        create(&store, "d").await;
        put(&store, "d", r#"{"id":"r","a":1,"b":2}"#).await;
        put(&store, "d", r#"{"id":"r","a":9}"#).await;

        let response = read_record(State(store.clone()), Path(("d".to_owned(), "r".to_owned()))).await;
        let body = body_json(response).await;
        // 整体替换：旧字段 b 消失，内容为最后一次写入。
        assert_eq!(body["record"], json!({"id": "r", "a": 9}));
    }

    #[tokio::test]
    async fn missing_record_or_dataset_returns_not_found() {
        let store = store();
        create(&store, "d").await;

        let response = read_record(State(store.clone()), Path(("d".to_owned(), "nope".to_owned()))).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let response = read_record(State(store.clone()), Path(("nope".to_owned(), "r".to_owned()))).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn malformed_record_requests_rejected_without_partial_writes() {
        let store = store();
        create(&store, "d").await;

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
        assert!(store.lock().unwrap().datasets["d"].records.is_empty());
    }

    #[tokio::test]
    async fn saved_version_is_immutable_against_rewrite_delete_and_insert() {
        let store = store();
        create(&store, "d").await;
        put(&store, "d", r#"{"id":"a","v":1}"#).await;
        put(&store, "d", r#"{"id":"b","v":1}"#).await;
        let v1 = save(&store, "d").await;

        // 保存后：覆盖 a、删除 b、新增 c，当前数据与版本内容都应不同。
        put(&store, "d", r#"{"id":"a","v":2}"#).await;
        let deleted = delete_record(State(store.clone()), Path(("d".to_owned(), "b".to_owned()))).await;
        assert_eq!(deleted.status(), StatusCode::OK);
        put(&store, "d", r#"{"id":"c","v":1}"#).await;

        // 当前数据反映最新状态。
        let current = read_record(State(store.clone()), Path(("d".to_owned(), "a".to_owned()))).await;
        assert_eq!(body_json(current).await["record"]["v"], 2);

        // 版本读回仍是保存那一刻的集合。
        let batch = read_version_records(
            State(store.clone()),
            Path(("d".to_owned(), v1.clone())),
        )
        .await;
        assert_eq!(batch.status(), StatusCode::OK);
        let batch = body_json(batch).await;
        assert_eq!(
            batch["records"],
            json!({
                "a": {"id":"a","v":1},
                "b": {"id":"b","v":1},
            })
        );

        let old_a = read_version_record(
            State(store.clone()),
            Path(("d".to_owned(), v1.clone(), "a".to_owned())),
        )
        .await;
        assert_eq!(body_json(old_a).await["record"]["v"], 1);

        let old_b = read_version_record(
            State(store.clone()),
            Path(("d".to_owned(), v1.clone(), "b".to_owned())),
        )
        .await;
        assert_eq!(old_b.status(), StatusCode::OK);

        // 版本中不存在后来新增的记录。
        let missing = read_version_record(
            State(store.clone()),
            Path(("d".to_owned(), v1, "c".to_owned())),
        )
        .await;
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn identical_snapshots_get_distinct_version_ids() {
        let store = store();
        create(&store, "d").await;
        put(&store, "d", r#"{"id":"a","v":1}"#).await;
        let v1 = save(&store, "d").await;
        let v2 = save(&store, "d").await;
        assert_ne!(v1, v2);

        // 两个版本内容完全一致：比较结果不含任何变化项。
        let response = compare_versions(
            State(store.clone()),
            Path("d".to_owned()),
            Bytes::from(format!(r#"{{"from":"{v1}","to":"{v2}"}}"#)),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        assert_eq!(body["added"], json!([]));
        assert_eq!(body["removed"], json!([]));
        assert_eq!(body["changed"], json!([]));
    }

    #[tokio::test]
    async fn compare_lists_added_removed_and_changed_and_reverses() {
        let store = store();
        create(&store, "d").await;
        put(&store, "d", r#"{"id":"keep","v":1}"#).await;
        put(&store, "d", r#"{"id":"gone","v":1}"#).await;
        put(&store, "d", r#"{"id":"edit","v":1}"#).await;
        let v1 = save(&store, "d").await;

        // 再次保存空版本也合法，但这里构造 v2：删除 gone、改写 edit、新增 born。
        let deleted = delete_record(State(store.clone()), Path(("d".to_owned(), "gone".to_owned()))).await;
        assert_eq!(deleted.status(), StatusCode::OK);
        put(&store, "d", r#"{"id":"edit","v":2}"#).await;
        put(&store, "d", r#"{"id":"born","v":1}"#).await;
        let v2 = save(&store, "d").await;

        let response = compare_versions(
            State(store.clone()),
            Path("d".to_owned()),
            Bytes::from(format!(r#"{{"from":"{v1}","to":"{v2}"}}"#)),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        assert_eq!(body["added"], json!([{"id":"born","record":{"id":"born","v":1}}]));
        assert_eq!(body["removed"], json!([{"id":"gone","record":{"id":"gone","v":1}}]));
        assert_eq!(
            body["changed"],
            json!([{
                "id": "edit",
                "before": {"id":"edit","v":1},
                "after": {"id":"edit","v":2},
            }])
        );

        // 反向比较：新增与删除互换，变更前后内容互换。
        let response = compare_versions(
            State(store.clone()),
            Path("d".to_owned()),
            Bytes::from(format!(r#"{{"from":"{v2}","to":"{v1}"}}"#)),
        )
        .await;
        let body = body_json(response).await;
        assert_eq!(body["added"][0]["id"], "gone");
        assert_eq!(body["removed"][0]["id"], "born");
        assert_eq!(body["changed"][0]["before"]["v"], 2);
        assert_eq!(body["changed"][0]["after"]["v"], 1);
    }

    #[tokio::test]
    async fn compare_with_unknown_version_or_dataset_fails_entirely() {
        let store = store();
        create(&store, "d").await;
        put(&store, "d", r#"{"id":"a","v":1}"#).await;
        let v1 = save(&store, "d").await;

        let response = compare_versions(
            State(store.clone()),
            Path("d".to_owned()),
            Bytes::from(format!(r#"{{"from":"{v1}","to":"nope"}}"#)),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let response = compare_versions(
            State(store.clone()),
            Path("ghost".to_owned()),
            Bytes::from(format!(r#"{{"from":"{v1}","to":"{v1}"}}"#)),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        for body in [r#"{"from":"v1"}"#, r#"{"to":"v1"}"#, r#"{}"#, r#"{"from":1,"to":"x"}"#] {
            let response = compare_versions(
                State(store.clone()),
                Path("d".to_owned()),
                Bytes::from(body),
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "body: {body}");
        }
    }

    #[tokio::test]
    async fn version_reads_return_not_found_without_placeholders() {
        let store = store();
        create(&store, "d").await;
        put(&store, "d", r#"{"id":"a","v":1}"#).await;
        save(&store, "d").await;

        // 不存在的版本
        let response = read_version_records(
            State(store.clone()),
            Path(("d".to_owned(), "v42".to_owned())),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        // 不存在的数据集
        let response = read_version_records(
            State(store.clone()),
            Path(("ghost".to_owned(), "v1".to_owned())),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        // 不存在的数据集上保存版本也失败，且不创建数据集
        let response = save_version(State(store.clone()), Path("ghost".to_owned())).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert!(store.lock().unwrap().datasets.get("ghost").is_none());
    }

    #[tokio::test]
    async fn delete_missing_record_or_dataset_returns_not_found() {
        let store = store();
        create(&store, "d").await;
        put(&store, "d", r#"{"id":"a","v":1}"#).await;

        let response =
            delete_record(State(store.clone()), Path(("d".to_owned(), "nope".to_owned()))).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let response =
            delete_record(State(store.clone()), Path(("ghost".to_owned(), "a".to_owned()))).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        // 已有数据不受影响。
        let response = read_record(State(store.clone()), Path(("d".to_owned(), "a".to_owned()))).await;
        assert_eq!(response.status(), StatusCode::OK);

        // 删除成功后当前读不到，但版本中仍可读到。
        let v1 = save(&store, "d").await;
        let response =
            delete_record(State(store.clone()), Path(("d".to_owned(), "a".to_owned()))).await;
        assert_eq!(response.status(), StatusCode::OK);
        let response = read_record(State(store.clone()), Path(("d".to_owned(), "a".to_owned()))).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let response = read_version_record(
            State(store.clone()),
            Path(("d".to_owned(), v1, "a".to_owned())),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn version_ids_are_unique_per_dataset() {
        let store = store();
        create(&store, "d1").await;
        create(&store, "d2").await;
        put(&store, "d1", r#"{"id":"x"}"#).await;
        put(&store, "d2", r#"{"id":"x"}"#).await;
        // 两个数据集各自从 v1 开始，互不影响；跨数据集引用对方版本须未找到。
        let a = save(&store, "d1").await;
        let b = save(&store, "d2").await;
        assert_eq!(a, "v1");
        assert_eq!(b, "v1");
        let v2 = save(&store, "d2").await;
        assert_eq!(v2, "v2");

        // d1 只有 v1，引用 d2 的 v2 必须未找到。
        let response = read_version_records(
            State(store.clone()),
            Path(("d1".to_owned(), v2)),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
