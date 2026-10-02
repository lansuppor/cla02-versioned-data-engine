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
async fn diff_endpoint_reports_changes_between_versions() {
    let dir = tempfile::TempDir::new();
    let v1;
    {
        let store = Arc::new(Store::open(dir.path()).unwrap());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            axum::serve(listener, app(store)).await.unwrap();
        });
        // v1：u1 将被改、u2 将被删、u3 保持不变。
        request(
            &addr,
            "POST",
            "/collections/users/records",
            Some(
                r#"{"records":[
                    {"id":"u1","name":"Ada","age":36},
                    {"id":"u2","name":"Lin"},
                    {"id":"u3","name":"Same"}
                ]}"#,
            ),
        );
        let r = request(&addr, "POST", "/versions", None);
        v1 = r.body["id"].as_u64().unwrap();
        // v1 之后新写入 u4（dropped 场景）。
        request(
            &addr,
            "POST",
            "/collections/users/records",
            Some(r#"{"records":[{"id":"u4","name":"New"}]}"#),
        );
        // 服务随作用域结束停止，释放 WAL 句柄。
    }

    // 改写快照当前数据：删除 u2、改写 u1、新增 u5，重启后保存 v2。
    let snap_path = dir.path().join("snapshot.json");
    let mut state: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&snap_path).unwrap()).unwrap();
    let recs = state["collections"][0]["records"].as_array_mut().unwrap();
    recs.retain(|r| r["id"] != "u2");
    for r in recs.iter_mut() {
        if r["id"] == "u1" {
            *r = json!({"id": "u1", "name": "Ada", "age": 37, "role": "admin"});
        }
    }
    recs.push(json!({"id": "u5", "name": "Later"}));
    let mut bytes = serde_json::to_vec(&state).unwrap();
    bytes.push(b'\n');
    std::fs::write(&snap_path, bytes).unwrap();

    let store = Arc::new(Store::open(dir.path()).unwrap());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        axum::serve(listener, app(store)).await.unwrap();
    });

    let r = request(&addr, "POST", "/versions", None);
    assert_eq!(r.status, 201);
    let v2 = r.body["id"].as_u64().unwrap();

    // 正常差异查询。
    let r = request(
        &addr,
        "GET",
        &format!("/versions/{v1}/collections/users/diff/{v2}"),
        None,
    );
    assert_eq!(r.status, 200, "{}", r.body);
    assert_eq!(r.body["from"], v1);
    assert_eq!(r.body["to"], v2);
    assert_eq!(r.body["collection"], "users");
    let added: Vec<_> = r.body["added"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["id"].clone())
        .collect();
    assert_eq!(added, vec![json!("u2")], "先写后删 → added");
    let dropped: Vec<_> = r.body["dropped"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["id"].clone())
        .collect();
    assert_eq!(dropped, vec![json!("u4"), json!("u5")], "新写入 → dropped");
    let changed = r.body["changed"].as_array().unwrap();
    assert_eq!(changed.len(), 1);
    assert_eq!(changed[0]["before"]["id"], "u1");
    assert_eq!(changed[0]["before"]["age"], 36);
    assert_eq!(changed[0]["after"]["age"], 37);
    assert_eq!(changed[0]["after"]["role"], "admin");

    // 同版本比较：三个列表均为空。
    let r = request(
        &addr,
        "GET",
        &format!("/versions/{v1}/collections/users/diff/{v1}"),
        None,
    );
    assert_eq!(r.status, 200);
    assert!(r.body["added"].as_array().unwrap().is_empty());
    assert!(r.body["dropped"].as_array().unwrap().is_empty());
    assert!(r.body["changed"].as_array().unwrap().is_empty());

    // 起始晚于目标：400。
    let r = request(
        &addr,
        "GET",
        &format!("/versions/{v2}/collections/users/diff/{v1}"),
        None,
    );
    assert_eq!(r.status, 400);
    let msg = r.body["error"].as_str().unwrap();
    assert!(msg.contains(&v1.to_string()) && msg.contains(&v2.to_string()));

    // 起始版本不存在：404。
    let r = request(
        &addr,
        "GET",
        &format!("/versions/999/collections/users/diff/{v2}"),
        None,
    );
    assert_eq!(r.status, 404);
    assert_eq!(r.body["error"], json!("version 999 not found"));

    // 目标版本不存在：404。
    let r = request(
        &addr,
        "GET",
        &format!("/versions/{v1}/collections/users/diff/999"),
        None,
    );
    assert_eq!(r.status, 404);
    assert_eq!(r.body["error"], json!("version 999 not found"));

    // 集合在起始版本不存在（orders 只在当前期创建）：404。
    request(
        &addr,
        "POST",
        "/collections/orders/records",
        Some(r#"{"records":[{"id":"o1"}]}"#),
    );
    let r = request(
        &addr,
        "GET",
        &format!("/versions/{v1}/collections/orders/diff/{v2}"),
        None,
    );
    assert_eq!(r.status, 404);
    assert_eq!(
        r.body["error"],
        json!(format!("collection \"orders\" not found in version {v1}"))
    );

    // 差异查询是只读的：再查一次 v1 视图，数据未变。
    let r = request(
        &addr,
        "GET",
        &format!("/versions/{v1}/collections/users/records"),
        None,
    );
    assert_eq!(r.body["records"].as_array().unwrap().len(), 3);

    // 顺序检查优先于集合检查：反向且集合缺失，仍报 400 而非 404。
    let r = request(
        &addr,
        "GET",
        &format!("/versions/{v2}/collections/missing/diff/{v1}"),
        None,
    );
    assert_eq!(r.status, 400);

    // 差异查询不生成新版本：版本总数仍为 2。
    let r = request(&addr, "GET", "/versions", None);
    assert_eq!(r.body["versions"].as_array().unwrap().len(), 2);
}

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
