//! HTTP 接口集成测试：通过真实 TCP 连接访问 axum 服务，
//! 覆盖健康/版本接口、写入校验、版本快照、重启恢复与并发场景。

use super::app;
use crate::store::Store;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;

use serde_json::{Value, json};

struct TestServer {
    addr: String,
    _dir: tempfile::TempDir,
}

mod tempfile {
    //! 极简临时目录（避免引入额外依赖）。
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static SEQ: AtomicU64 = AtomicU64::new(0);

    pub struct TempDir(pub PathBuf);
    impl TempDir {
        pub fn new() -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let pid = std::process::id();
            // 全局递增序号，杜绝同纳秒内并发创建导致的目录名撞车。
            let seq = SEQ.fetch_add(1, Ordering::Relaxed);
            let dir = std::env::temp_dir().join(format!("vde-http-{pid}-{nanos}-{seq}"));
            std::fs::create_dir_all(&dir).unwrap();
            TempDir(dir)
        }
        pub fn path(&self) -> &std::path::Path {
            &self.0
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

async fn spawn_server() -> TestServer {
    let dir = tempfile::TempDir::new();
    let store = Arc::new(Store::open(dir.path()).unwrap());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        axum::serve(listener, app(store)).await.unwrap();
    });
    TestServer { addr, _dir: dir }
}

struct RawResponse {
    status: u16,
    body: Value,
}

fn request(addr: &str, method: &str, path: &str, body: Option<&str>) -> RawResponse {
    let mut stream = TcpStream::connect(addr).unwrap();
    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
    if let Some(b) = body {
        req.push_str("Content-Type: application/json\r\n");
        req.push_str(&format!("Content-Length: {}\r\n", b.len()));
    }
    req.push_str("\r\n");
    if let Some(b) = body {
        req.push_str(b);
    }
    stream.write_all(req.as_bytes()).unwrap();
    let mut raw = String::new();
    stream.read_to_string(&mut raw).unwrap();
    let split = raw.find("\r\n\r\n").unwrap();
    let head = &raw[..split];
    let text = &raw[split + 4..];
    let status: u16 = head
        .lines()
        .next()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    RawResponse {
        status,
        body: serde_json::from_str(text.trim()).unwrap_or(Value::Null),
    }
}

// HTTP 测试通过阻塞式 std TCP 发请求，因此统一使用多线程运行时，
// 避免 current-thread 运行时中请求方占满工作线程、服务端无法响应的死锁。

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn health_and_version_unchanged() {
    let s = spawn_server().await;
    let h = request(&s.addr, "GET", "/health", None);
    assert_eq!(h.status, 200);
    assert_eq!(h.body, json!({"status": "ok"}));

    let v = request(&s.addr, "GET", "/version", None);
    assert_eq!(v.status, 200);
    assert_eq!(
        v.body,
        json!({"name": "versioned-data-engine", "version": "0.1.0"})
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_save_and_query_versioned_data() {
    let s = spawn_server().await;

    // 首次写入自动创建集合。
    let r = request(
        &s.addr,
        "POST",
        "/collections/users/records",
        Some(
            r#"{"records":[{"id":"u1","name":"Ada","age":36,"active":true,"meta":{"role":"admin","score":10}},{"id":"u2","name":"Lin","age":28,"active":false,"meta":null}]}"#,
        ),
    );
    assert_eq!(r.status, 200, "{}", r.body);
    assert_eq!(r.body["inserted"], 2);

    // 非法整批：u3 是浮点，u4 不得写入。
    let r = request(
        &s.addr,
        "POST",
        "/collections/users/records",
        Some(r#"{"records":[{"id":"u4","x":1},{"id":"u3","x":1.5}]}"#),
    );
    assert_eq!(r.status, 400);
    assert_eq!(r.body["index"], 1);
    assert_eq!(r.body["id"], "u3");
    assert!(r.body["error"].as_str().unwrap().contains("integer"));

    // 缺主键。
    let r = request(
        &s.addr,
        "POST",
        "/collections/users/records",
        Some(r#"{"records":[{"name":"no-id"}]}"#),
    );
    assert_eq!(r.status, 400);
    assert!(r.body["error"].as_str().unwrap().contains("id"));

    // 同批主键重复。
    let r = request(
        &s.addr,
        "POST",
        "/collections/users/records",
        Some(r#"{"records":[{"id":"d"},{"id":"d"}]}"#),
    );
    assert_eq!(r.status, 400);
    assert!(r.body["error"].as_str().unwrap().contains("duplicate"));

    // 同主键不同内容 → 拒绝。
    let r = request(
        &s.addr,
        "POST",
        "/collections/users/records",
        Some(
            r#"{"records":[{"id":"u1","name":"Ada","age":37,"active":true,"meta":{"role":"admin","score":10}}]}"#,
        ),
    );
    assert_eq!(r.status, 400);
    assert!(
        r.body["error"]
            .as_str()
            .unwrap()
            .contains("different record")
    );

    // 相同内容重放：幂等，inserted=0。
    let r = request(
        &s.addr,
        "POST",
        "/collections/users/records",
        Some(
            r#"{"records":[{"id":"u1","name":"Ada","age":36,"active":true,"meta":{"role":"admin","score":10}}]}"#,
        ),
    );
    assert_eq!(r.status, 200);
    assert_eq!(r.body["inserted"], 0);

    // 保存版本 1。
    let r = request(&s.addr, "POST", "/versions", None);
    assert_eq!(r.status, 201);
    let v1 = r.body["id"].as_u64().unwrap();
    assert!(r.body["saved_at"].as_u64().is_some());

    // 后续写入不影响已保存版本。
    request(
        &s.addr,
        "POST",
        "/collections/users/records",
        Some(r#"{"records":[{"id":"u5","name":"New"}]}"#),
    );
    request(
        &s.addr,
        "POST",
        "/collections/orders/records",
        Some(r#"{"records":[{"id":"o1","total":5}]}"#),
    );
    let r = request(&s.addr, "POST", "/versions", None);
    let v2 = r.body["id"].as_u64().unwrap();

    // v1 视图仍为保存时的 2 条，字段顺序与写入一致。
    let r = request(
        &s.addr,
        "GET",
        &format!("/versions/{v1}/collections/users/records"),
        None,
    );
    assert_eq!(r.status, 200);
    let recs = r.body["records"].as_array().unwrap();
    assert_eq!(recs.len(), 2);
    let keys: Vec<_> = recs[0].as_object().unwrap().keys().cloned().collect();
    assert_eq!(keys, vec!["id", "name", "age", "active", "meta"]);
    assert_eq!(recs[0]["id"], "u1");
    assert_eq!(recs[1]["id"], "u2");

    // v2 含 3 个用户与新集合。
    let r = request(
        &s.addr,
        "GET",
        &format!("/versions/{v2}/collections/users/records"),
        None,
    );
    assert_eq!(r.body["records"].as_array().unwrap().len(), 3);
    let r = request(
        &s.addr,
        "GET",
        &format!("/versions/{v2}/collections/orders/records"),
        None,
    );
    assert_eq!(r.body["records"][0]["id"], "o1");

    // v1 中不存在 orders 集合。
    let r = request(
        &s.addr,
        "GET",
        &format!("/versions/{v1}/collections/orders/records"),
        None,
    );
    assert_eq!(r.status, 404);
    assert!(r.body["error"].as_str().unwrap().contains("orders"));

    // 不存在的版本。
    let r = request(
        &s.addr,
        "GET",
        "/versions/999/collections/users/records",
        None,
    );
    assert_eq!(r.status, 404);
    assert!(r.body["error"].as_str().unwrap().contains("version"));

    // 列表与详情。
    let r = request(&s.addr, "GET", "/versions", None);
    assert_eq!(r.body["versions"].as_array().unwrap().len(), 2);
    let r = request(&s.addr, "GET", &format!("/versions/{v2}"), None);
    assert_eq!(r.status, 200);
    let cols = r.body["collections"].as_array().unwrap();
    assert_eq!(cols[0], "orders");
    assert_eq!(cols[1], "users");

    // 非法 JSON / 空批次。
    let r = request(
        &s.addr,
        "POST",
        "/collections/users/records",
        Some("{not json"),
    );
    assert_eq!(r.status, 400);
    let r = request(
        &s.addr,
        "POST",
        "/collections/users/records",
        Some(r#"{"records":[]}"#),
    );
    assert_eq!(r.status, 400);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_versions_endpoint() {
    let s = spawn_server().await;

    // v1：两个用户。
    let r = request(
        &s.addr,
        "POST",
        "/collections/users/records",
        Some(
            r#"{"records":[{"id":"u1","name":"Ada","age":36},{"id":"u2","name":"Lin","age":28}]}"#,
        ),
    );
    assert_eq!(r.status, 200);
    let r = request(&s.addr, "POST", "/versions", None);
    let v1 = r.body["id"].as_u64().unwrap();

    // v2：新增 u3（同主键不同内容会被写入接口拒绝，故 dropped 是唯一可经接口产生的变化）。
    let r = request(
        &s.addr,
        "POST",
        "/collections/users/records",
        Some(r#"{"records":[{"id":"u3","name":"New","age":20}]}"#),
    );
    assert_eq!(r.status, 200);
    let r = request(
        &s.addr,
        "POST",
        "/collections/orders/records",
        Some(r#"{"records":[{"id":"o1","total":5}]}"#),
    );
    assert_eq!(r.status, 200);
    let r = request(&s.addr, "POST", "/versions", None);
    let v2 = r.body["id"].as_u64().unwrap();

    // v1 → v2：u3 出现在 dropped，added/changed 为空。
    let r = request(
        &s.addr,
        "GET",
        &format!("/versions/{v1}/collections/users/diff/{v2}"),
        None,
    );
    assert_eq!(r.status, 200, "{}", r.body);
    assert_eq!(r.body["from"], v1);
    assert_eq!(r.body["to"], v2);
    assert_eq!(r.body["collection"], "users");
    assert_eq!(r.body["added"].as_array().unwrap().len(), 0);
    assert_eq!(r.body["changed"].as_array().unwrap().len(), 0);
    let dropped = r.body["dropped"].as_array().unwrap();
    assert_eq!(dropped.len(), 1);
    assert_eq!(dropped[0]["id"], "u3");

    // 起始与目标相同 → 三个列表都为空。
    let r = request(
        &s.addr,
        "GET",
        &format!("/versions/{v1}/collections/users/diff/{v1}"),
        None,
    );
    assert_eq!(r.status, 200);
    assert_eq!(r.body["added"].as_array().unwrap().len(), 0);
    assert_eq!(r.body["dropped"].as_array().unwrap().len(), 0);
    assert_eq!(r.body["changed"].as_array().unwrap().len(), 0);

    // 起始版本晚于目标版本 → 400。
    let r = request(
        &s.addr,
        "GET",
        &format!("/versions/{v2}/collections/users/diff/{v1}"),
        None,
    );
    assert_eq!(r.status, 400);
    assert!(r.body["error"].as_str().unwrap().contains("later"));

    // 版本不存在 → 404。
    let r = request(
        &s.addr,
        "GET",
        &format!("/versions/999/collections/users/diff/{v2}"),
        None,
    );
    assert_eq!(r.status, 404);
    assert!(r.body["error"].as_str().unwrap().contains("version 999"));
    let r = request(
        &s.addr,
        "GET",
        &format!("/versions/{v1}/collections/users/diff/999"),
        None,
    );
    assert_eq!(r.status, 404);
    assert!(r.body["error"].as_str().unwrap().contains("version 999"));

    // 集合在起始版本中不存在 → 404。
    let r = request(
        &s.addr,
        "GET",
        &format!("/versions/{v1}/collections/orders/diff/{v2}"),
        None,
    );
    assert_eq!(r.status, 404);
    assert!(
        r.body["error"]
            .as_str()
            .unwrap()
            .contains(&format!("version {v1}"))
    );

    // 顺序检查优先于集合检查：v2 → v1 即使集合缺失也先报 400。
    let r = request(
        &s.addr,
        "GET",
        &format!("/versions/{v2}/collections/orders/diff/{v1}"),
        None,
    );
    assert_eq!(r.status, 400);

    // 集合在起始与目标版本中都不存在 → 404。
    let r = request(
        &s.addr,
        "GET",
        &format!("/versions/{v1}/collections/orders/diff/{v1}"),
        None,
    );
    assert_eq!(r.status, 404);
    assert!(r.body["error"].as_str().unwrap().contains("orders"));

    // 差异查询不生成新版本。
    let r = request(&s.addr, "GET", "/versions", None);
    assert_eq!(r.body["versions"].as_array().unwrap().len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persisted_versions_survive_restart() {
    let dir = tempfile::TempDir::new();
    let v1;
    {
        let store = Arc::new(Store::open(dir.path()).unwrap());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            axum::serve(listener, app(store)).await.unwrap();
        });
        request(
            &addr,
            "POST",
            "/collections/users/records",
            Some(r#"{"records":[{"id":"u1","v":1}]}"#),
        );
        let r = request(&addr, "POST", "/versions", None);
        v1 = r.body["id"].as_u64().unwrap();
        request(
            &addr,
            "POST",
            "/collections/users/records",
            Some(r#"{"records":[{"id":"u2","v":2}]}"#),
        );
        // 服务随作用域结束而停止。
    }

    // 用同一数据目录重新打开并启动。
    let store = Arc::new(Store::open(dir.path()).unwrap());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        axum::serve(listener, app(store)).await.unwrap();
    });

    let r = request(
        &addr,
        "GET",
        &format!("/versions/{v1}/collections/users/records"),
        None,
    );
    assert_eq!(r.status, 200);
    assert_eq!(r.body["records"].as_array().unwrap().len(), 1);

    // 保存后未打快照的提交（u2）也通过 WAL 恢复。
    let r = request(&addr, "POST", "/versions", None);
    let v2 = r.body["id"].as_u64().unwrap();
    let r = request(
        &addr,
        "GET",
        &format!("/versions/{v2}/collections/users/records"),
        None,
    );
    assert_eq!(r.body["records"].as_array().unwrap().len(), 2);
    let r = request(&addr, "GET", "/versions", None);
    assert_eq!(r.body["versions"].as_array().unwrap().len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replace_records_endpoint() {
    let s = spawn_server().await;

    let post = |body: &'static str| {
        let addr = s.addr.clone();
        tokio::task::spawn_blocking(move || {
            request(&addr, "POST", "/collections/users/records", Some(body))
        })
    };

    // 初始数据。
    let r = post(
        r#"{"records":[{"id":"u1","name":"Ada","age":36},{"id":"u2","name":"Lin","age":28}]}"#,
    )
    .await
    .unwrap();
    assert_eq!(r.status, 200, "{}", r.body);
    assert_eq!(r.body["inserted"], 2);
    // 缺省模式下响应不含 replaced 字段（行为与原来完全一致）。
    assert!(r.body.get("replaced").is_none());

    // 保存版本 1。
    let r = request(&s.addr, "POST", "/versions", None);
    let v1 = r.body["id"].as_u64().unwrap();

    // replace=false 显式给出：同主键不同内容仍拒绝，且不返回 replaced。
    let r = post(r#"{"replace":false,"records":[{"id":"u1","age":37}]}"#)
        .await
        .unwrap();
    assert_eq!(r.status, 400);
    assert!(
        r.body["error"]
            .as_str()
            .unwrap()
            .contains("different record")
    );

    // replace=true：u1 被替换，u3 新增；u2 不变。
    let r = post(r#"{"replace":true,"records":[{"id":"u1","name":"Ada","age":37,"role":"lead"},{"id":"u3","name":"New","age":20}]}"#)
        .await
        .unwrap();
    assert_eq!(r.status, 200, "{}", r.body);
    assert_eq!(r.body["accepted"], 2);
    assert_eq!(r.body["inserted"], 1);
    assert_eq!(r.body["replaced"], 1);

    // 内容完全相同的替换批次：幂等，inserted/replaced 均为 0。
    let r = post(r#"{"replace":true,"records":[{"id":"u1","name":"Ada","age":37,"role":"lead"}]}"#)
        .await
        .unwrap();
    assert_eq!(r.status, 200);
    assert_eq!(r.body["inserted"], 0);
    assert_eq!(r.body["replaced"], 0);

    // 字段顺序不同但内容相同：幂等，不替换。
    let r = post(r#"{"replace":true,"records":[{"role":"lead","id":"u1","age":37,"name":"Ada"}]}"#)
        .await
        .unwrap();
    assert_eq!(r.body["replaced"], 0);

    // 替换后字段顺序按新记录保留。
    let r = post(r#"{"replace":true,"records":[{"age":38,"id":"u1","name":"Ada","role":"lead"}]}"#)
        .await
        .unwrap();
    assert_eq!(r.body["replaced"], 1);

    // 同批主键重复：即使 replace=true 也整批拒绝。
    let r = post(r#"{"replace":true,"records":[{"id":"d","v":1},{"id":"d","v":2}]}"#)
        .await
        .unwrap();
    assert_eq!(r.status, 400);
    assert!(r.body["error"].as_str().unwrap().contains("duplicate"));

    // 非法记录（浮点 / 数组 / 非对象 / 缺 id）：replace=true 仍整批拒绝，无部分替换。
    for bad in [
        r#"{"replace":true,"records":[{"id":"u1","age":1},{"id":"bad","x":1.5}]}"#,
        r#"{"replace":true,"records":[{"id":"u1","age":1},{"id":"bad","t":[1]}]}"#,
        r#"{"replace":true,"records":[42]}"#,
        r#"{"replace":true,"records":[{"no":"id"}]}"#,
    ] {
        let r = post(bad).await.unwrap();
        assert_eq!(r.status, 400, "body: {}", r.body);
        assert!(r.body.get("index").is_some());
        assert!(r.body.get("error").is_some());
    }

    // 空批次仍拒绝。
    let r = post(r#"{"replace":true,"records":[]}"#).await.unwrap();
    assert_eq!(r.status, 400);

    // 保存版本 2：验证历史版本不变、当前数据为替换后内容、差异中 u1 出现在 changed。
    let r = request(&s.addr, "POST", "/versions", None);
    let v2 = r.body["id"].as_u64().unwrap();

    let r = request(
        &s.addr,
        "GET",
        &format!("/versions/{v1}/collections/users/records"),
        None,
    );
    let old: Vec<_> = r.body["records"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|rec| rec["id"] == "u1")
        .cloned()
        .collect();
    assert_eq!(old[0]["age"], 36);
    assert!(old[0].get("role").is_none());

    let r = request(
        &s.addr,
        "GET",
        &format!("/versions/{v2}/collections/users/records"),
        None,
    );
    let recs = r.body["records"].as_array().unwrap();
    assert_eq!(recs.len(), 3);
    let u1 = recs.iter().find(|rec| rec["id"] == "u1").unwrap();
    assert_eq!(u1["age"], 38);
    let keys: Vec<_> = u1.as_object().unwrap().keys().cloned().collect();
    assert_eq!(keys, vec!["age", "id", "name", "role"]);

    let r = request(
        &s.addr,
        "GET",
        &format!("/versions/{v1}/collections/users/diff/{v2}"),
        None,
    );
    assert_eq!(r.status, 200, "{}", r.body);
    let changed = r.body["changed"].as_array().unwrap();
    assert_eq!(changed.len(), 1);
    assert_eq!(changed[0]["id"], "u1");
    assert_eq!(changed[0]["before"]["age"], 36);
    assert_eq!(changed[0]["after"]["age"], 38);
    let dropped: Vec<_> = r.body["dropped"]
        .as_array()
        .unwrap()
        .iter()
        .map(|rec| rec["id"].as_str().unwrap())
        .collect();
    assert_eq!(dropped, vec!["u3"]);
    assert_eq!(r.body["added"].as_array().unwrap().len(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replace_survives_restart_over_http() {
    let dir = tempfile::TempDir::new();
    let v1;
    {
        let store = Arc::new(Store::open(dir.path()).unwrap());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            axum::serve(listener, app(store)).await.unwrap();
        });
        request(
            &addr,
            "POST",
            "/collections/users/records",
            Some(r#"{"records":[{"id":"u1","v":1},{"id":"u2","v":2}]}"#),
        );
        v1 = request(&addr, "POST", "/versions", None).body["id"]
            .as_u64()
            .unwrap();
        // 已确认的替换：WAL 追加并 fsync 后才返回 200。
        let r = request(
            &addr,
            "POST",
            "/collections/users/records",
            Some(r#"{"replace":true,"records":[{"id":"u1","v":100}]}"#),
        );
        assert_eq!(r.status, 200);
        assert_eq!(r.body["replaced"], 1);
    }

    // 同一数据目录重启。
    let store = Arc::new(Store::open(dir.path()).unwrap());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        axum::serve(listener, app(store)).await.unwrap();
    });

    // 历史版本仍是旧记录。
    let r = request(
        &addr,
        "GET",
        &format!("/versions/{v1}/collections/users/records"),
        None,
    );
    let u1 = r.body["records"]
        .as_array()
        .unwrap()
        .iter()
        .find(|rec| rec["id"] == "u1")
        .unwrap()
        .clone();
    assert_eq!(u1["v"], 1);

    // 当前数据与替换后一致。
    let v2 = request(&addr, "POST", "/versions", None).body["id"]
        .as_u64()
        .unwrap();
    let r = request(
        &addr,
        "GET",
        &format!("/versions/{v2}/collections/users/records"),
        None,
    );
    let recs = r.body["records"].as_array().unwrap();
    assert_eq!(recs.len(), 2);
    let u1 = recs.iter().find(|rec| rec["id"] == "u1").unwrap();
    assert_eq!(u1["v"], 100);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writes_and_snapshots_never_see_half_batches() {
    let s = Arc::new(spawn_server().await);
    let writers = 8usize;
    let batches = 12usize;
    let batch_size = 5usize;

    let mut handles = Vec::new();
    for w in 0..writers {
        let addr = s.addr.clone();
        handles.push(tokio::spawn(async move {
            for b in 0..batches {
                let mut recs = String::from("[");
                for i in 0..batch_size {
                    if i > 0 {
                        recs.push(',');
                    }
                    recs.push_str(&format!(
                        "{{\"id\":\"w{w}-b{b}-r{i}\",\"w\":{w},\"b\":{b},\"i\":{i}}}"
                    ));
                }
                recs.push(']');
                let payload = format!(r#"{{"records":{recs}}}"#);
                let addr2 = addr.clone();
                let r = tokio::task::spawn_blocking(move || {
                    request(&addr2, "POST", "/collections/items/records", Some(&payload))
                })
                .await
                .unwrap();
                assert_eq!(r.status, 200, "batch {w}/{b}: {}", r.body);
            }
        }));
    }

    // 并发持续保存快照。
    let saver_addr = s.addr.clone();
    let saver = tokio::spawn(async move {
        let mut saved = Vec::new();
        for _ in 0..20 {
            let addr2 = saver_addr.clone();
            let r = tokio::task::spawn_blocking(move || request(&addr2, "POST", "/versions", None))
                .await
                .unwrap();
            assert_eq!(r.status, 201);
            saved.push(r.body["id"].as_u64().unwrap());
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        saved
    });

    for h in handles {
        h.await.unwrap();
    }
    let saved = saver.await.unwrap();

    // 每个快照中，任一批次要么完整出现、要么完全不出现。
    for vid in saved {
        let addr = s.addr.clone();
        let r = tokio::task::spawn_blocking(move || {
            request(
                &addr,
                "GET",
                &format!("/versions/{vid}/collections/items/records"),
                None,
            )
        })
        .await
        .unwrap();
        assert_eq!(r.status, 200);
        // 以 (w,b) 分组计数，每个已出现的批次必须是完整的 batch_size 条。
        let mut counts = std::collections::HashMap::new();
        for rec in r.body["records"].as_array().unwrap() {
            let key = (rec["w"].as_u64().unwrap(), rec["b"].as_u64().unwrap());
            *counts.entry(key).or_insert(0u64) += 1;
        }
        for ((w, b), n) in &counts {
            assert_eq!(
                *n, batch_size as u64,
                "half batch w{w} b{b}: {n}/{batch_size}"
            );
        }
    }

    // 最终全部写入均可见（无丢失的已确认批次）。
    let r = request(&s.addr, "POST", "/versions", None);
    let final_v = r.body["id"].as_u64().unwrap();
    let r = request(
        &s.addr,
        "GET",
        &format!("/versions/{final_v}/collections/items/records"),
        None,
    );
    assert_eq!(
        r.body["records"].as_array().unwrap().len(),
        writers * batches * batch_size
    );
}
