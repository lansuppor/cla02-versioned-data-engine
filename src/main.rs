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
    collections::{BTreeMap, HashMap},
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
/// - `records`：当前最新记录（标识 -> 记录内容）。
/// - `versions`：已保存的不可变快照（版本标识 -> 保存那一刻的记录集合）。
/// - `version_seq`：数据集内单调递增的版本序号，保证版本标识在数据集内唯一；
///   即使两次保存内容完全一致，也会得到不同标识。
///
/// 快照使用 BTreeMap，使整批读取与比较结果按记录标识稳定排序。
/// 所有变更在单个互斥锁内完成"校验 + 写入"，保证每次请求原子生效。
#[derive(Default)]
struct Dataset {
    records: HashMap<String, Value>,
    versions: BTreeMap<String, BTreeMap<String, Value>>,
    version_seq: u64,
}

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

/// 从 JSON 对象中取出字符串字段；缺失或类型不符返回 400。
fn required_string<'a>(
    object: &'a Map<String, Value>,
    field: &str,
) -> Result<&'a str, Response> {
    match object.get(field).and_then(Value::as_str) {
        Some(value) => Ok(value),
        None => Err(error(
            StatusCode::BAD_REQUEST,
            format!("field \"{field}\" is required and must be a string"),
        )),
    }
}

async fn create_dataset(State(store): State<SharedStore>, body: Bytes) -> Response {
    let object = match parse_json_object(&body) {
        Ok(object) => object,
        Err(response) => return response,
    };
    let name = match required_string(&object, "name") {
        Ok(name) => name,
        Err(response) => return response,
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
    let id = match required_string(&object, "id") {
        Ok(id) => id.to_owned(),
        Err(response) => return response,
    };

    let mut store = store.lock().expect("store mutex poisoned");
    let Some(dataset_state) = store.datasets.get_mut(&dataset) else {
        return error(
            StatusCode::NOT_FOUND,
            format!("dataset \"{dataset}\" not found"),
        );
    };
    // 同一标识整体替换，其余记录不受影响；已保存的版本保持不变。
    dataset_state.records.insert(id.clone(), Value::Object(object));
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
    let Some(dataset_state) = store.datasets.get(&dataset) else {
        return error(
            StatusCode::NOT_FOUND,
            format!("dataset \"{dataset}\" not found"),
        );
    };
    let Some(record) = dataset_state.records.get(&id) else {
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
    let Some(dataset_state) = store.datasets.get_mut(&dataset) else {
        return error(
            StatusCode::NOT_FOUND,
            format!("dataset \"{dataset}\" not found"),
        );
    };
    // 仅从当前数据移除；已保存版本中的快照不受影响。
    if dataset_state.records.remove(&id).is_none() {
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

/// 为数据集当前的全部记录建立不可变快照。整个过程在一次锁内完成：
/// 成功则产生一个可查询的版本，失败（数据集不存在）则不写任何状态。
async fn save_version(
    State(store): State<SharedStore>,
    Path(dataset): Path<String>,
) -> Response {
    let mut store = store.lock().expect("store mutex poisoned");
    let Some(dataset_state) = store.datasets.get_mut(&dataset) else {
        return error(
            StatusCode::NOT_FOUND,
            format!("dataset \"{dataset}\" not found"),
        );
    };
    dataset_state.version_seq += 1;
    let version = format!("v{}", dataset_state.version_seq);
    let snapshot: BTreeMap<String, Value> = dataset_state
        .records
        .iter()
        .map(|(id, record)| (id.clone(), record.clone()))
        .collect();
    let record_count = snapshot.len();
    dataset_state.versions.insert(version.clone(), snapshot);
    (
        StatusCode::CREATED,
        Json(json!({
            "dataset": dataset,
            "version": version,
            "record_count": record_count,
        })),
    )
        .into_response()
}

/// 按版本读取整批记录，还原保存那一刻的记录集合。
async fn read_version_records(
    State(store): State<SharedStore>,
    Path((dataset, version)): Path<(String, String)>,
) -> Response {
    let store = store.lock().expect("store mutex poisoned");
    let Some(dataset_state) = store.datasets.get(&dataset) else {
        return error(
            StatusCode::NOT_FOUND,
            format!("dataset \"{dataset}\" not found"),
        );
    };
    let Some(snapshot) = dataset_state.versions.get(&version) else {
        return error(
            StatusCode::NOT_FOUND,
            format!("version \"{version}\" not found in dataset \"{dataset}\""),
        );
    };
    let records: Vec<&Value> = snapshot.values().collect();
    (
        StatusCode::OK,
        Json(json!({
            "dataset": dataset,
            "version": version,
            "records": records,
        })),
    )
        .into_response()
}

/// 按版本读取单条记录。
async fn read_version_record(
    State(store): State<SharedStore>,
    Path((dataset, version, id)): Path<(String, String, String)>,
) -> Response {
    let store = store.lock().expect("store mutex poisoned");
    let Some(dataset_state) = store.datasets.get(&dataset) else {
        return error(
            StatusCode::NOT_FOUND,
            format!("dataset \"{dataset}\" not found"),
        );
    };
    let Some(snapshot) = dataset_state.versions.get(&version) else {
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

/// 比较同一数据集的两个版本。
///
/// 请求体 `{"from":"前一版本","to":"后一版本"}`，顺序由调用方指定：
///
/// - 仅存在于 to：新增；仅存在于 from：删除；两边都有但内容不同：内容变更。
/// - 引用的任一版本不存在时整体 404，不产生任何部分结果。
async fn compare_versions(
    State(store): State<SharedStore>,
    Path(dataset): Path<String>,
    body: Bytes,
) -> Response {
    let object = match parse_json_object(&body) {
        Ok(object) => object,
        Err(response) => return response,
    };
    let from = match required_string(&object, "from") {
        Ok(from) => from.to_owned(),
        Err(response) => return response,
    };
    let to = match required_string(&object, "to") {
        Ok(to) => to.to_owned(),
        Err(response) => return response,
    };

    let store = store.lock().expect("store mutex poisoned");
    let Some(dataset_state) = store.datasets.get(&dataset) else {
        return error(
            StatusCode::NOT_FOUND,
            format!("dataset \"{dataset}\" not found"),
        );
    };
    // 先整体校验两个版本都存在，再做比较，杜绝部分结果。
    let Some(from_snapshot) = dataset_state.versions.get(&from) else {
        return error(
            StatusCode::NOT_FOUND,
            format!("version \"{from}\" not found in dataset \"{dataset}\""),
        );
    };
    let Some(to_snapshot) = dataset_state.versions.get(&to) else {
        return error(
            StatusCode::NOT_FOUND,
            format!("version \"{to}\" not found in dataset \"{dataset}\""),
        );
    };

    // 以两个快照标识的并集遍历，BTreeMap 保证输出按标识排序、结果稳定。
    let mut added: Vec<Value> = Vec::new();
    let mut removed: Vec<Value> = Vec::new();
    let mut changed: Vec<Value> = Vec::new();
    for (id, to_record) in to_snapshot {
        match from_snapshot.get(id) {
            None => added.push(json!({ "id": id, "record": to_record })),
            Some(from_record) if from_record != to_record => {
                changed.push(json!({ "id": id, "before": from_record, "after": to_record }));
            }
            Some(_) => {}
        }
    }
    for (id, from_record) in from_snapshot {
        if !to_snapshot.contains_key(id) {
            removed.push(json!({ "id": id, "record": from_record }));
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
        .route(
            "/datasets/{dataset}/records/{id}",
            get(read_record).delete(delete_record),
        )
        .route("/datasets/{dataset}/versions", post(save_version))
        .route(
            "/datasets/{dataset}/versions/compare",
            post(compare_versions),
        )
        .route(
            "/datasets/{dataset}/versions/{version}/records",
            get(read_version_records),
        )
        .route(
            "/datasets/{dataset}/versions/{version}/records/{id}",
            get(read_version_record),
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

    async fn create_dataset_named(store: &SharedStore, name: &str) {
        let response =
            create_dataset(State(store.clone()), Bytes::from(format!(r#"{{"name":"{name}"}}"#)))
                .await;
        assert_eq!(response.status(), StatusCode::CREATED);
    }

    async fn put_record(store: &SharedStore, dataset: &str, record: &str) {
        let response = write_record(
            State(store.clone()),
            Path(dataset.to_owned()),
            Bytes::from(record.to_owned()),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    async fn save(store: &SharedStore, dataset: &str) -> (StatusCode, Value) {
        let response = save_version(State(store.clone()), Path(dataset.to_owned())).await;
        let status = response.status();
        (status, body_json(response).await)
    }

    async fn compare(store: &SharedStore, dataset: &str, body: &str) -> (StatusCode, Value) {
        let response = compare_versions(
            State(store.clone()),
            Path(dataset.to_owned()),
            Bytes::from(body.to_owned()),
        )
        .await;
        let status = response.status();
        (status, body_json(response).await)
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
        assert!(store.lock().unwrap().datasets["d"].records.is_empty());
    }

    #[tokio::test]
    async fn saved_version_is_immutable_against_later_writes_and_deletes() {
        let store = store();
        create_dataset_named(&store, "d").await;
        put_record(&store, "d", r#"{"id":"r1","v":1}"#).await;
        put_record(&store, "d", r#"{"id":"r2","v":"old"}"#).await;

        let (status, body) = save(&store, "d").await;
        assert_eq!(status, StatusCode::CREATED);
        let v1 = body["version"].as_str().unwrap().to_owned();
        assert_eq!(body["record_count"], 2);

        // 保存之后：替换 r2、删除 r1、新增 r3。
        put_record(&store, "d", r#"{"id":"r2","v":"new"}"#).await;
        let response = delete_record(
            State(store.clone()),
            Path(("d".to_owned(), "r1".to_owned())),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        put_record(&store, "d", r#"{"id":"r3","v":3}"#).await;

        // 当前数据：r1 已不存在。
        let response = read_record(State(store.clone()), Path(("d".to_owned(), "r1".to_owned()))).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        // 版本整批读回：仍是保存那一刻的两条记录。
        let response = read_version_records(
            State(store.clone()),
            Path(("d".to_owned(), v1.clone())),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        assert_eq!(
            body["records"],
            json!([
                {"id": "r1", "v": 1},
                {"id": "r2", "v": "old"}
            ])
        );

        // 版本单条读回：已删除的 r1 仍可读到，r2 仍是旧内容。
        let response = read_version_record(
            State(store.clone()),
            Path(("d".to_owned(), v1.clone(), "r1".to_owned())),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_json(response).await["record"], json!({"id": "r1", "v": 1}));

        let response = read_version_record(
            State(store.clone()),
            Path(("d".to_owned(), v1.clone(), "r2".to_owned())),
        )
        .await;
        assert_eq!(body_json(response).await["record"], json!({"id": "r2", "v": "old"}));

        // 版本中不存在的记录、版本之后新增的记录都读不到。
        let response = read_version_record(
            State(store.clone()),
            Path(("d".to_owned(), v1.clone(), "r3".to_owned())),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn identical_snapshots_get_distinct_version_ids() {
        let store = store();
        create_dataset_named(&store, "d").await;
        put_record(&store, "d", r#"{"id":"r1","v":1}"#).await;

        let (_, first) = save(&store, "d").await;
        let (_, second) = save(&store, "d").await;
        assert_ne!(first["version"], second["version"]);

        // 两个版本都能独立查询，内容一致。
        for body in [first, second] {
            let version = body["version"].as_str().unwrap().to_owned();
            let response = read_version_records(
                State(store.clone()),
                Path(("d".to_owned(), version)),
            )
            .await;
            assert_eq!(
                body_json(response).await["records"],
                json!([{"id": "r1", "v": 1}])
            );
        }
    }

    #[tokio::test]
    async fn saving_version_on_missing_dataset_fails_atomically() {
        let store = store();
        let response = save_version(State(store.clone()), Path("ghost".to_owned())).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        // 不创建空数据集或占位版本。
        assert!(store.lock().unwrap().datasets.is_empty());
    }

    #[tokio::test]
    async fn empty_dataset_can_be_snapshotted_and_read_back() {
        let store = store();
        create_dataset_named(&store, "d").await;
        let (status, body) = save(&store, "d").await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(body["record_count"], 0);

        let version = body["version"].as_str().unwrap().to_owned();
        let response = read_version_records(
            State(store.clone()),
            Path(("d".to_owned(), version)),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_json(response).await["records"], json!([]));
    }

    #[tokio::test]
    async fn reading_unknown_version_or_dataset_returns_not_found() {
        let store = store();
        create_dataset_named(&store, "d").await;
        put_record(&store, "d", r#"{"id":"r1"}"#).await;
        let (_, body) = save(&store, "d").await;
        let v1 = body["version"].as_str().unwrap().to_owned();

        // 数据集不存在。
        let response = read_version_records(
            State(store.clone()),
            Path(("ghost".to_owned(), v1.clone())),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        // 版本不存在。
        let response = read_version_records(
            State(store.clone()),
            Path(("d".to_owned(), "v999".to_owned())),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let response = read_version_record(
            State(store.clone()),
            Path(("d".to_owned(), "v999".to_owned(), "r1".to_owned())),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn compare_lists_added_removed_and_changed() {
        let store = store();
        create_dataset_named(&store, "d").await;
        put_record(&store, "d", r#"{"id":"keep","v":1}"#).await;
        put_record(&store, "d", r#"{"id":"gone","v":2}"#).await;
        put_record(&store, "d", r#"{"id":"edit","v":3}"#).await;
        let (_, v1_body) = save(&store, "d").await;
        let v1 = v1_body["version"].as_str().unwrap().to_owned();

        put_record(&store, "d", r#"{"id":"edit","v":30}"#).await;
        let response = delete_record(
            State(store.clone()),
            Path(("d".to_owned(), "gone".to_owned())),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        put_record(&store, "d", r#"{"id":"born","v":4}"#).await;
        let (_, v2_body) = save(&store, "d").await;
        let v2 = v2_body["version"].as_str().unwrap().to_owned();

        let (status, body) =
            compare(&store, "d", &format!(r#"{{"from":"{v1}","to":"{v2}"}}"#)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["from"], v1);
        assert_eq!(body["to"], v2);
        assert_eq!(body["added"], json!([{"id": "born", "record": {"id": "born", "v": 4}}]));
        assert_eq!(body["removed"], json!([{"id": "gone", "record": {"id": "gone", "v": 2}}]));
        assert_eq!(
            body["changed"],
            json!([{
                "id": "edit",
                "before": {"id": "edit", "v": 3},
                "after": {"id": "edit", "v": 30},
            }])
        );
        // 未变化的记录不出现在任何列表中。
        for list in ["added", "removed", "changed"] {
            let ids: Vec<&str> = body[list]
                .as_array()
                .unwrap()
                .iter()
                .map(|item| item["id"].as_str().unwrap())
                .collect();
            assert!(!ids.contains(&"keep"));
        }
    }

    #[tokio::test]
    async fn compare_reversed_swaps_added_removed_and_before_after() {
        let store = store();
        create_dataset_named(&store, "d").await;
        put_record(&store, "d", r#"{"id":"a","v":1}"#).await;
        let (_, b1) = save(&store, "d").await;
        let v1 = b1["version"].as_str().unwrap().to_owned();

        put_record(&store, "d", r#"{"id":"a","v":2}"#).await;
        put_record(&store, "d", r#"{"id":"b","v":1}"#).await;
        let (_, b2) = save(&store, "d").await;
        let v2 = b2["version"].as_str().unwrap().to_owned();

        let (_, forward) =
            compare(&store, "d", &format!(r#"{{"from":"{v1}","to":"{v2}"}}"#)).await;
        let (_, reverse) =
            compare(&store, "d", &format!(r#"{{"from":"{v2}","to":"{v1}"}}"#)).await;

        assert_eq!(forward["added"][0]["id"], "b");
        assert!(forward["removed"].as_array().unwrap().is_empty());
        assert_eq!(reverse["removed"][0]["id"], "b");
        assert!(reverse["added"].as_array().unwrap().is_empty());

        assert_eq!(forward["changed"][0]["before"], json!({"id": "a", "v": 1}));
        assert_eq!(forward["changed"][0]["after"], json!({"id": "a", "v": 2}));
        assert_eq!(reverse["changed"][0]["before"], json!({"id": "a", "v": 2}));
        assert_eq!(reverse["changed"][0]["after"], json!({"id": "a", "v": 1}));
    }

    #[tokio::test]
    async fn compare_identical_versions_returns_empty_changes() {
        let store = store();
        create_dataset_named(&store, "d").await;
        put_record(&store, "d", r#"{"id":"r1","v":1}"#).await;
        let (_, b1) = save(&store, "d").await;
        let v1 = b1["version"].as_str().unwrap().to_owned();
        let (_, b2) = save(&store, "d").await;
        let v2 = b2["version"].as_str().unwrap().to_owned();

        let (status, body) =
            compare(&store, "d", &format!(r#"{{"from":"{v1}","to":"{v2}"}}"#)).await;
        assert_eq!(status, StatusCode::OK);
        for list in ["added", "removed", "changed"] {
            assert!(body[list].as_array().unwrap().is_empty(), "list {list} should be empty");
        }

        // 版本与自身比较也没有任何变化。
        let (_, self_body) =
            compare(&store, "d", &format!(r#"{{"from":"{v1}","to":"{v1}"}}"#)).await;
        for list in ["added", "removed", "changed"] {
            assert!(self_body[list].as_array().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn compare_with_unknown_version_fails_entirely_with_reason() {
        let store = store();
        create_dataset_named(&store, "d").await;
        put_record(&store, "d", r#"{"id":"r1","v":1}"#).await;
        let (_, b1) = save(&store, "d").await;
        let v1 = b1["version"].as_str().unwrap().to_owned();

        // from 不存在。
        let body_text = format!(r#"{{"from":"v404","to":"{v1}"}}"#);
        let (status, body) = compare(&store, "d", &body_text).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(body["error"].as_str().unwrap().contains("v404"));

        // to 不存在。
        let (status, body) =
            compare(&store, "d", &format!(r#"{{"from":"{v1}","to":"v404"}}"#)).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(body["error"].as_str().unwrap().contains("v404"));

        // 数据集不存在。
        let (status, _) =
            compare(&store, "ghost", &format!(r#"{{"from":"{v1}","to":"{v1}"}}"#)).await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        // 请求体非法 / 字段缺失：400。
        for bad in ["not json", r#"[1]"#, r#"{"from":"v1"}"#, r#"{"from":1,"to":"v2"}"#] {
            let (status, _) = compare(&store, "d", bad).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "body: {bad}");
        }
    }

    #[tokio::test]
    async fn delete_missing_record_or_dataset_returns_not_found() {
        let store = store();
        create_dataset_named(&store, "d").await;

        let response = delete_record(
            State(store.clone()),
            Path(("d".to_owned(), "ghost-record".to_owned())),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert!(store.lock().unwrap().datasets["d"].records.is_empty());

        let response = delete_record(
            State(store.clone()),
            Path(("ghost".to_owned(), "r1".to_owned())),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert!(!store.lock().unwrap().datasets.contains_key("ghost"));
    }
}
