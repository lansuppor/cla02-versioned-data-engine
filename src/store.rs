//! 持久化存储：集合、当前数据状态与不可变版本快照。
//!
//! 持久化采用“快照文件 + 追加式 WAL”：
//! - `snapshot.json` 以“临时文件 + 原子重命名”落盘，包含全部已保存版本及其快照数据、
//!   以及保存时刻各集合的当前数据；
//! - `wal.log` 逐行追加已确认提交的批量写入（每行一个 JSON，以换行结尾）；
//! - 启动时先恢复快照，再顺序重放 WAL；末尾若存在崩溃残留的残缺行则截断，
//!   因此未完成（未 fsync）的写入在重启后不会产生任何记录。
//!
//! 所有提交与保存都在同一把 `Mutex` 下完成，保证线性一致：快照要么包含
//! 整批已提交写入，要么完全不包含，不会出现半批数据。

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

/// 一条记录即保留字段顺序的 JSON 对象，主键为其中的 `id` 字段。
pub type Record = Map<String, Value>;

/// 一次写入被业务规则拒绝：定位到批次内的记录位置、主键（若可识别）与原因。
#[derive(Debug, Clone, Serialize)]
pub struct Rejection {
    pub index: usize,
    pub id: Option<String>,
    pub reason: String,
}

/// 写入失败：整批被拒绝（400）或持久化失败（500）。
#[derive(Debug)]
pub enum BatchError {
    Rejected(Rejection),
    Persist(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LookupError {
    VersionNotFound,
    CollectionNotFound,
}

/// 版本差异查询失败：版本/集合不存在（404）或起始版本晚于目标版本（400）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiffError {
    FromVersionNotFound,
    ToVersionNotFound,
    CollectionNotFoundInFrom,
    CollectionNotFoundInTo,
    /// 起始版本晚于目标版本（版本号随保存动作递增）。
    OutOfOrder,
}

/// 两个版本中都存在但内容不同的一条记录：修改前与修改后的完整记录。
#[derive(Debug, Clone, Serialize)]
pub struct ChangedRecord {
    pub before: Record,
    pub after: Record,
}

/// 同一集合在两个已保存版本之间的差异；三个列表均按主键 id 字节序升序。
#[derive(Debug, Clone, Serialize)]
pub struct DiffOutcome {
    pub from: u64,
    pub to: u64,
    pub collection: String,
    /// 仅存在于起始版本的记录（在目标版本中被删除）。
    pub added: Vec<Record>,
    /// 仅存在于目标版本的记录（新写入）。
    pub dropped: Vec<Record>,
    /// 两个版本中都存在但内容不同的记录。
    pub changed: Vec<ChangedRecord>,
}

#[derive(Debug, Clone, Serialize)]
pub struct WriteOutcome {
    pub collection: String,
    /// 批次中被接受的记录数（等于整批大小）。
    pub accepted: usize,
    /// 实际新增的记录数（与已有记录完全相同的写入幂等，不计入）。
    pub inserted: usize,
    /// 被新内容覆盖的已有记录数（仅 replace=true 时可能大于 0；
    /// 与已有记录完全相同的写入幂等，不计入）。
    pub replaced: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct VersionSummary {
    pub id: u64,
    /// 保存时间（Unix 纪元毫秒）。
    pub saved_at: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct VersionDetail {
    pub id: u64,
    pub saved_at: u64,
    /// 该版本快照中包含的集合名。
    pub collections: Vec<String>,
}

/// 一个已保存版本的完整只读快照。
struct VersionSnap {
    id: u64,
    saved_at: u64,
    data: HashMap<String, Vec<Record>>,
}

/// WAL 中的事件：已确认提交的一批记录。
#[derive(Serialize, Deserialize)]
struct CommitEvent {
    collection: String,
    records: Vec<Record>,
}

#[derive(Serialize, Deserialize)]
struct PersistedCollection {
    name: String,
    records: Vec<Record>,
}

#[derive(Serialize, Deserialize)]
struct PersistedVersion {
    id: u64,
    saved_at: u64,
    collections: Vec<PersistedCollection>,
}

/// `snapshot.json` 的完整内容。
#[derive(Serialize, Deserialize)]
struct PersistedState {
    versions: Vec<PersistedVersion>,
    collections: Vec<PersistedCollection>,
}

struct Inner {
    /// 各集合的当前数据；Vec 保存提交顺序。
    current: BTreeMap<String, Vec<Record>>,
    versions: Vec<VersionSnap>,
    next_id: u64,
    wal: fs::File,
}

/// 整个服务的状态。
pub struct Store {
    inner: Mutex<Inner>,
    data_dir: PathBuf,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl Store {
    /// 从数据目录加载或初始化存储，并完成崩溃恢复。
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self, String> {
        let data_dir = data_dir.as_ref().to_path_buf();
        fs::create_dir_all(&data_dir).map_err(|e| format!("create data dir: {e}"))?;

        let mut current: BTreeMap<String, Vec<Record>> = BTreeMap::new();
        let mut versions: Vec<VersionSnap> = Vec::new();

        // 1. 恢复最近一次快照（若存在）。
        let snap_path = data_dir.join("snapshot.json");
        if snap_path.exists() {
            let bytes = fs::read(&snap_path).map_err(|e| format!("read snapshot: {e}"))?;
            let state: PersistedState =
                serde_json::from_slice(&bytes).map_err(|e| format!("parse snapshot: {e}"))?;
            for c in state.collections {
                current.insert(c.name, c.records);
            }
            for v in state.versions {
                versions.push(VersionSnap {
                    id: v.id,
                    saved_at: v.saved_at,
                    data: v
                        .collections
                        .into_iter()
                        .map(|c| (c.name, c.records))
                        .collect(),
                });
            }
        }
        let next_id = versions.last().map(|v| v.id + 1).unwrap_or(1);

        // 2. 重放快照之后的 WAL，并修复崩溃残留的残缺行。
        let wal_path = data_dir.join("wal.log");
        let mut valid_bytes = 0usize;
        if wal_path.exists() {
            let content = fs::read_to_string(&wal_path).map_err(|e| format!("read wal: {e}"))?;
            let mut rest = content.as_str();
            loop {
                if rest.is_empty() {
                    break;
                }
                let Some(nl) = rest.find('\n') else {
                    // 最后一行没有换行结尾：本服务的提交总是以换行结尾，故必为残缺行。
                    break;
                };
                let line = &rest[..nl];
                match serde_json::from_str::<CommitEvent>(line) {
                    Ok(event) => {
                        apply_commit(&mut current, event.collection, event.records);
                        valid_bytes += nl + 1;
                        rest = &rest[nl + 1..];
                    }
                    Err(_) => break,
                }
            }
            if valid_bytes < content.len() {
                let f = fs::OpenOptions::new()
                    .write(true)
                    .open(&wal_path)
                    .map_err(|e| format!("repair wal: {e}"))?;
                f.set_len(valid_bytes as u64)
                    .map_err(|e| format!("repair wal: {e}"))?;
                f.sync_all().ok();
            }
        }

        // 3. 打开 WAL 供后续追加。
        let wal = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path)
            .map_err(|e| format!("open wal: {e}"))?;

        Ok(Store {
            inner: Mutex::new(Inner {
                current,
                versions,
                next_id,
                wal,
            }),
            data_dir,
        })
    }

    /// 批量写入：整批校验通过后原子提交，任一条不合法则整批拒绝、已有数据不变。
    ///
    /// `replace=false`（默认）时同主键不同内容整批拒绝；`replace=true` 时允许用
    /// 新内容覆盖同主键的已有记录。无论哪种模式，同一请求内主键重复、非法记录等
    /// 仍然整批拒绝，不会出现部分写入/替换。
    pub fn write_batch(
        &self,
        collection: &str,
        batch: &[Value],
        replace: bool,
    ) -> Result<WriteOutcome, BatchError> {
        if collection.is_empty() {
            return Err(BatchError::Rejected(Rejection {
                index: 0,
                id: None,
                reason: "collection name must not be empty".to_string(),
            }));
        }

        let mut st = self.inner.lock().unwrap();

        // 阶段 1：逐条校验；任何失败都直接返回，状态未做任何修改。
        let mut seen: HashMap<&str, usize> = HashMap::with_capacity(batch.len());
        let mut prepared: Vec<Record> = Vec::with_capacity(batch.len());
        for (i, raw) in batch.iter().enumerate() {
            let rec = match raw {
                Value::Object(map) => map,
                _ => {
                    return Err(BatchError::Rejected(Rejection {
                        index: i,
                        id: None,
                        reason: "record must be a JSON object".to_string(),
                    }));
                }
            };
            let id = match rec.get("id") {
                None => {
                    return Err(BatchError::Rejected(Rejection {
                        index: i,
                        id: None,
                        reason: "missing primary key field \"id\"".to_string(),
                    }));
                }
                Some(Value::String(s)) if !s.is_empty() => s.as_str(),
                Some(Value::String(_)) => {
                    return Err(BatchError::Rejected(Rejection {
                        index: i,
                        id: None,
                        reason: "primary key \"id\" must be a non-empty string".to_string(),
                    }));
                }
                Some(_) => {
                    return Err(BatchError::Rejected(Rejection {
                        index: i,
                        id: None,
                        reason: "primary key \"id\" must be a string".to_string(),
                    }));
                }
            };
            if let Some(&first) = seen.get(id) {
                return Err(BatchError::Rejected(Rejection {
                    index: i,
                    id: Some(id.to_string()),
                    reason: format!(
                        "duplicate id \"{id}\" in the same batch (first at index {first})"
                    ),
                }));
            }
            for (field, value) in rec.iter() {
                if let Err(msg) = validate_value(value) {
                    return Err(BatchError::Rejected(Rejection {
                        index: i,
                        id: Some(id.to_string()),
                        reason: format!("field \"{field}\": {msg}"),
                    }));
                }
            }
            seen.insert(id, i);
            prepared.push(rec.clone());
        }

        // 阶段 2：与集合中已有记录冲突检查——replace=false 时同主键但内容不同即
        // 拒绝整批；replace=true 时跳过（阶段 3 改为覆盖）。
        if !replace && let Some(existing) = st.current.get(collection) {
            for (i, rec) in prepared.iter().enumerate() {
                let id = rec["id"].as_str().unwrap();
                if let Some(old) = existing.iter().find(|r| r["id"] == id)
                    && old != rec
                {
                    return Err(BatchError::Rejected(Rejection {
                        index: i,
                        id: Some(id.to_string()),
                        reason: format!(
                            "collection \"{collection}\" already contains a different record with id \"{id}\""
                        ),
                    }));
                }
            }
        }

        // 阶段 3：先持久化（追加 WAL 并 fsync），成功后才更新内存并确认成功。
        let event = CommitEvent {
            collection: collection.to_string(),
            records: prepared.clone(),
        };
        append_wal(&mut st.wal, &event).map_err(BatchError::Persist)?;

        let existing = st.current.entry(collection.to_string()).or_default();
        let mut inserted = 0usize;
        let mut replaced = 0usize;
        for rec in prepared {
            let id = rec["id"].as_str().unwrap();
            match existing.iter().position(|r| r["id"] == id) {
                Some(pos) => {
                    if existing[pos] != rec {
                        // 内容不同：仅在 replace=true 时可能走到这里，计入 replaced；
                        // 内容相同为幂等写入，两个计数都不增加。
                        replaced += 1;
                    }
                    existing[pos] = rec;
                }
                None => {
                    existing.push(rec);
                    inserted += 1;
                }
            }
        }

        Ok(WriteOutcome {
            collection: collection.to_string(),
            accepted: batch.len(),
            inserted,
            replaced,
        })
    }

    /// 把当前各集合数据固化为只读版本，返回版本标识与保存时间。
    pub fn save_version(&self) -> Result<VersionSummary, String> {
        let mut st = self.inner.lock().unwrap();
        let id = st.next_id;
        let saved_at = now_ms();
        let data: HashMap<String, Vec<Record>> = st
            .current
            .iter()
            .map(|(name, records)| (name.clone(), records.clone()))
            .collect();
        st.versions.push(VersionSnap { id, saved_at, data });
        st.next_id += 1;

        // 固化完整状态到快照文件，然后清空 WAL（旧提交已包含在快照中）。
        persist_state(&self.data_dir, &st)?;
        truncate_wal(&mut st.wal, &self.data_dir.join("wal.log"))?;

        Ok(VersionSummary { id, saved_at })
    }

    /// 查询指定版本内指定集合的全部记录（字段顺序与写入时一致）。
    pub fn read_records(
        &self,
        version_id: u64,
        collection: &str,
    ) -> Result<Vec<Record>, LookupError> {
        let st = self.inner.lock().unwrap();
        let version = st
            .versions
            .iter()
            .find(|v| v.id == version_id)
            .ok_or(LookupError::VersionNotFound)?;
        version
            .data
            .get(collection)
            .cloned()
            .ok_or(LookupError::CollectionNotFound)
    }

    /// 比较同一集合在两个已保存版本之间的差异。
    ///
    /// 只读取快照，不生成新版本、不改写任何数据。记录内容比较与写入冲突检测一致：
    /// 字段顺序不影响判定，字段名与字段值相同即视为同一条记录。
    pub fn diff_versions(
        &self,
        from: u64,
        to: u64,
        collection: &str,
    ) -> Result<DiffOutcome, DiffError> {
        let st = self.inner.lock().unwrap();
        let from_v = st
            .versions
            .iter()
            .find(|v| v.id == from)
            .ok_or(DiffError::FromVersionNotFound)?;
        let to_v = st
            .versions
            .iter()
            .find(|v| v.id == to)
            .ok_or(DiffError::ToVersionNotFound)?;
        if from > to {
            return Err(DiffError::OutOfOrder);
        }
        let from_recs = from_v
            .data
            .get(collection)
            .ok_or(DiffError::CollectionNotFoundInFrom)?;
        let to_recs = to_v
            .data
            .get(collection)
            .ok_or(DiffError::CollectionNotFoundInTo)?;

        // BTreeMap 迭代顺序即主键 id 的字节序升序。
        let from_map: BTreeMap<&str, &Record> = from_recs
            .iter()
            .map(|r| (r["id"].as_str().expect("validated record"), r))
            .collect();
        let to_map: BTreeMap<&str, &Record> = to_recs
            .iter()
            .map(|r| (r["id"].as_str().expect("validated record"), r))
            .collect();

        let mut added = Vec::new();
        let mut dropped = Vec::new();
        let mut changed = Vec::new();
        for (id, before) in &from_map {
            match to_map.get(id) {
                None => added.push((*before).clone()),
                Some(after) if **after != **before => changed.push(ChangedRecord {
                    before: (*before).clone(),
                    after: (*after).clone(),
                }),
                Some(_) => {}
            }
        }
        for (id, after) in &to_map {
            if !from_map.contains_key(id) {
                dropped.push((*after).clone());
            }
        }

        Ok(DiffOutcome {
            from,
            to,
            collection: collection.to_string(),
            added,
            dropped,
            changed,
        })
    }

    /// 已保存版本列表（按保存顺序）。
    pub fn list_versions(&self) -> Vec<VersionSummary> {
        let st = self.inner.lock().unwrap();
        st.versions
            .iter()
            .map(|v| VersionSummary {
                id: v.id,
                saved_at: v.saved_at,
            })
            .collect()
    }

    /// 单个版本的详情（含集合名列表）。
    pub fn get_version(&self, version_id: u64) -> Result<VersionDetail, LookupError> {
        let st = self.inner.lock().unwrap();
        let v = st
            .versions
            .iter()
            .find(|v| v.id == version_id)
            .ok_or(LookupError::VersionNotFound)?;
        Ok(VersionDetail {
            id: v.id,
            saved_at: v.saved_at,
            collections: v.data.keys().cloned().collect(),
        })
    }
}

/// WAL 重放/应用：按主键 upsert；整批来自同一个 commit 事件。
fn apply_commit(
    current: &mut BTreeMap<String, Vec<Record>>,
    collection: String,
    records: Vec<Record>,
) {
    let entry = current.entry(collection).or_default();
    for rec in records {
        let id = rec["id"].as_str().expect("validated commit");
        match entry.iter().position(|r| r["id"] == id) {
            Some(pos) => entry[pos] = rec,
            None => entry.push(rec),
        }
    }
}

fn append_wal(wal: &mut fs::File, event: &CommitEvent) -> Result<(), String> {
    let mut line = serde_json::to_vec(event).map_err(|e| format!("encode wal: {e}"))?;
    line.push(b'\n');
    wal.write_all(&line)
        .and_then(|_| wal.flush())
        .and_then(|_| wal.sync_all())
        .map_err(|e| format!("wal append: {e}"))
}

/// 完整状态写入 snapshot.json（临时文件 + fsync + 原子重命名 + 目录 fsync）。
fn persist_state(dir: &Path, st: &Inner) -> Result<(), String> {
    let state = PersistedState {
        versions: st
            .versions
            .iter()
            .map(|v| PersistedVersion {
                id: v.id,
                saved_at: v.saved_at,
                collections: v
                    .data
                    .iter()
                    .map(|(name, records)| PersistedCollection {
                        name: name.clone(),
                        records: records.clone(),
                    })
                    .collect(),
            })
            .collect(),
        collections: st
            .current
            .iter()
            .map(|(name, records)| PersistedCollection {
                name: name.clone(),
                records: records.clone(),
            })
            .collect(),
    };
    let mut bytes = serde_json::to_vec(&state).map_err(|e| format!("encode snapshot: {e}"))?;
    bytes.push(b'\n');

    let target = dir.join("snapshot.json");
    let tmp = dir.join("snapshot.json.tmp");
    {
        let mut f = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)
            .map_err(|e| format!("write snapshot tmp: {e}"))?;
        f.write_all(&bytes)
            .and_then(|_| f.flush())
            .and_then(|_| f.sync_all())
            .map_err(|e| format!("write snapshot tmp: {e}"))?;
    }
    fs::rename(&tmp, &target).map_err(|e| format!("rename snapshot: {e}"))?;
    if let Ok(d) = fs::File::open(dir) {
        let _ = d.sync_all();
    }
    Ok(())
}

/// 清空 WAL（其中的提交已固化进 snapshot.json），并重新以追加模式打开。
fn truncate_wal(wal: &mut fs::File, wal_path: &Path) -> Result<(), String> {
    {
        let mut f = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(wal_path)
            .map_err(|e| format!("truncate wal: {e}"))?;
        f.flush()
            .and_then(|_| f.sync_all())
            .map_err(|e| format!("truncate wal: {e}"))?;
    }
    // 先关闭旧句柄，再以 append 重新打开供后续提交使用。
    let reopened = fs::OpenOptions::new()
        .append(true)
        .open(wal_path)
        .map_err(|e| format!("reopen wal: {e}"))?;
    *wal = reopened;
    Ok(())
}

/// 递归校验字段值：仅允许字符串、整数、布尔、null 与嵌套对象；
/// 浮点数与数组一律拒绝。
fn validate_value(v: &Value) -> Result<(), String> {
    match v {
        Value::Null | Value::Bool(_) | Value::String(_) => Ok(()),
        Value::Number(n) if n.is_i64() || n.is_u64() => Ok(()),
        Value::Number(_) => Err("number must be an integer".to_string()),
        Value::Object(map) => {
            for (k, child) in map {
                validate_value(child).map_err(|e| format!("{k}: {e}"))?;
            }
            Ok(())
        }
        Value::Array(_) => Err("arrays are not allowed".to_string()),
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use serde_json::json;

    fn temp_dir() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let mut dir = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        dir.push(format!("vde-test-{}-{nanos}-{seq}", std::process::id()));
        dir
    }

    #[test]
    fn rejects_batch_atomically_with_detailed_reason() {
        let dir = temp_dir();
        let store = Store::open(&dir).unwrap();
        let ok = store
            .write_batch(
                "users",
                &[json!({"id": "u1", "name": "Ada", "age": 36})],
                false,
            )
            .unwrap();
        assert_eq!(ok.inserted, 1);

        // 第二条非法，整批拒绝；u2 不得残留。
        let err = store
            .write_batch(
                "users",
                &[
                    json!({"id": "u2", "score": 100}),
                    json!({"id": "u3", "score": 9.5}),
                ],
                false,
            )
            .unwrap_err();
        match err {
            BatchError::Rejected(r) => {
                assert_eq!(r.index, 1);
                assert_eq!(r.id.as_deref(), Some("u3"));
                assert!(r.reason.contains("integer"));
            }
            BatchError::Persist(e) => panic!("{e}"),
        }

        // 缺主键。
        let err = store
            .write_batch("users", &[json!({"name": "NoId"})], false)
            .unwrap_err();
        match err {
            BatchError::Rejected(r) => assert!(r.reason.contains("missing primary key")),
            _ => panic!(),
        }

        // 同批重复主键。
        let err = store
            .write_batch("users", &[json!({"id": "d1"}), json!({"id": "d1"})], false)
            .unwrap_err();
        match err {
            BatchError::Rejected(r) => assert!(r.reason.contains("duplicate id")),
            _ => panic!(),
        }

        // 同主键不同内容 → 拒绝。
        let err = store
            .write_batch(
                "users",
                &[json!({"id": "u1", "name": "Ada", "age": 37})],
                false,
            )
            .unwrap_err();
        match err {
            BatchError::Rejected(r) => assert!(r.reason.contains("different record")),
            _ => panic!(),
        }

        // 数组字段拒绝。
        let err = store
            .write_batch("users", &[json!({"id": "u4", "tags": ["a"]})], false)
            .unwrap_err();
        match err {
            BatchError::Rejected(r) => assert!(r.reason.contains("arrays")),
            _ => panic!(),
        }

        // u2 未写入。
        let v = store.save_version().unwrap();
        let recs = store.read_records(v.id, "users").unwrap();
        assert_eq!(recs.len(), 1);

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn snapshots_are_immutable_and_field_order_preserved() {
        let dir = temp_dir();
        let store = Store::open(&dir).unwrap();
        store
            .write_batch(
                "users",
                &[json!({"id": "u1", "z": 1, "a": true, "nested": {"y": 2, "b": null}})],
                false,
            )
            .unwrap();
        let v1 = store.save_version().unwrap();

        // 覆盖写入并清空式替换（同内容幂等，不同内容拒绝，用新集合模拟后续变化）。
        store
            .write_batch("users", &[json!({"id": "u2", "x": 2})], false)
            .unwrap();
        let _v2 = store.save_version().unwrap();

        let old = store.read_records(v1.id, "users").unwrap();
        assert_eq!(old.len(), 1);
        let keys: Vec<_> = old[0].keys().cloned().collect();
        assert_eq!(keys, vec!["id", "z", "a", "nested"]);
        let nested: Vec<_> = old[0]["nested"]
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect();
        assert_eq!(nested, vec!["y", "b"]);

        assert!(matches!(
            store.read_records(999, "users"),
            Err(LookupError::VersionNotFound)
        ));
        assert!(matches!(
            store.read_records(v1.id, "missing"),
            Err(LookupError::CollectionNotFound)
        ));

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn diff_versions_reports_added_dropped_changed_sorted() {
        let dir = temp_dir();
        let store = Store::open(&dir).unwrap();

        // v1：三条记录；b 的字段顺序与 v2 不同但内容相同，不应算作 changed。
        store
            .write_batch(
                "users",
                &[
                    json!({"id": "b", "x": 1, "y": 2}),
                    json!({"id": "gone", "v": 1}),
                    json!({"id": "mod", "v": 1}),
                ],
                false,
            )
            .unwrap();
        let v1 = store.save_version().unwrap();

        // v2：gone 被删除、mod 内容变化、new 新写入（写入接口不产生删除/修改，
        // 直接注入快照模拟这两个方向）。
        store
            .write_batch("users", &[json!({"id": "new", "v": 9})], false)
            .unwrap();
        let v2 = store.save_version().unwrap();
        {
            let mut st = store.inner.lock().unwrap();
            let data = st.versions[1].data.get_mut("users").unwrap();
            data.retain(|r| r["id"] != "gone");
            let m = data.iter_mut().find(|r| r["id"] == "mod").unwrap();
            m.insert("v".to_string(), json!(2));
            let b = data.iter_mut().find(|r| r["id"] == "b").unwrap();
            *b = json!({"y": 2, "id": "b", "x": 1})
                .as_object()
                .unwrap()
                .clone();
        }

        let d = store.diff_versions(v1.id, v2.id, "users").unwrap();
        let added_ids: Vec<_> = d.added.iter().map(|r| r["id"].as_str().unwrap()).collect();
        assert_eq!(added_ids, vec!["gone"]);
        assert_eq!(
            Value::Object(d.added[0].clone()),
            json!({"id": "gone", "v": 1})
        );
        let dropped_ids: Vec<_> = d
            .dropped
            .iter()
            .map(|r| r["id"].as_str().unwrap())
            .collect();
        assert_eq!(dropped_ids, vec!["new"]);
        assert_eq!(d.changed.len(), 1);
        assert_eq!(
            Value::Object(d.changed[0].before.clone()),
            json!({"id": "mod", "v": 1})
        );
        assert_eq!(
            Value::Object(d.changed[0].after.clone()),
            json!({"id": "mod", "v": 2})
        );

        // 起始版本晚于目标版本 → OutOfOrder。
        assert_eq!(
            store.diff_versions(v2.id, v1.id, "users").unwrap_err(),
            DiffError::OutOfOrder
        );

        // 起始与目标相同 → 三个列表都为空。
        let d = store.diff_versions(v1.id, v1.id, "users").unwrap();
        assert!(d.added.is_empty() && d.dropped.is_empty() && d.changed.is_empty());

        // 版本或集合不存在。
        assert_eq!(
            store.diff_versions(999, v1.id, "users").unwrap_err(),
            DiffError::FromVersionNotFound
        );
        assert_eq!(
            store.diff_versions(v1.id, 999, "users").unwrap_err(),
            DiffError::ToVersionNotFound
        );
        assert_eq!(
            store.diff_versions(v1.id, v2.id, "missing").unwrap_err(),
            DiffError::CollectionNotFoundInFrom
        );

        // 差异查询不生成新版本。
        assert_eq!(store.list_versions().len(), 2);

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn diff_versions_sorts_all_lists_by_id_bytes() {
        let dir = temp_dir();
        let store = Store::open(&dir).unwrap();
        store
            .write_batch(
                "c",
                &[
                    json!({"id": "z1"}),
                    json!({"id": "a2"}),
                    json!({"id": "a10"}),
                    json!({"id": "m1", "v": 1}),
                    json!({"id": "m0", "v": 1}),
                ],
                false,
            )
            .unwrap();
        let v1 = store.save_version().unwrap();
        store
            .write_batch("c", &[json!({"id": "n2"}), json!({"id": "n10"})], false)
            .unwrap();
        let v2 = store.save_version().unwrap();
        {
            let mut st = store.inner.lock().unwrap();
            let data = st.versions[1].data.get_mut("c").unwrap();
            data.retain(|r| !matches!(r["id"].as_str(), Some("z1") | Some("a2") | Some("a10")));
            for r in data.iter_mut() {
                if r["id"] == "m1" || r["id"] == "m0" {
                    r.insert("v".to_string(), json!(2));
                }
            }
        }

        let d = store.diff_versions(v1.id, v2.id, "c").unwrap();
        // 字节序升序："a10" < "a2" < "z1"，"m0" < "m1"，"n10" < "n2"。
        let ids = |rs: &[Record]| {
            rs.iter()
                .map(|r| r["id"].as_str().unwrap().to_string())
                .collect::<Vec<_>>()
        };
        assert_eq!(ids(&d.added), vec!["a10", "a2", "z1"]);
        assert_eq!(ids(&d.dropped), vec!["n10", "n2"]);
        let changed_ids: Vec<_> = d
            .changed
            .iter()
            .map(|c| c.before["id"].as_str().unwrap())
            .collect();
        assert_eq!(changed_ids, vec!["m0", "m1"]);

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn recovers_state_after_restart() {
        let dir = temp_dir();
        let v1_id;
        {
            let store = Store::open(&dir).unwrap();
            store
                .write_batch("users", &[json!({"id": "u1", "v": 1})], false)
                .unwrap();
            v1_id = store.save_version().unwrap().id;
            store
                .write_batch("users", &[json!({"id": "u2", "v": 2})], false)
                .unwrap();
        }
        // 重新打开：旧版本数据与保存后的 WAL 提交都应可见。
        let store = Store::open(&dir).unwrap();
        let recs = store.read_records(v1_id, "users").unwrap();
        assert_eq!(recs.len(), 1);
        let versions = store.list_versions();
        assert_eq!(versions.len(), 1);
        let v2 = store.save_version().unwrap();
        assert_eq!(store.read_records(v2.id, "users").unwrap().len(), 2);

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn truncated_wal_tail_is_discarded() {
        let dir = temp_dir();
        {
            let store = Store::open(&dir).unwrap();
            store
                .write_batch("users", &[json!({"id": "u1"})], false)
                .unwrap();
            store.save_version().unwrap();
            store
                .write_batch("users", &[json!({"id": "u2"})], false)
                .unwrap();
        }
        // 模拟崩溃：追加一段不完整的行。
        let wal_path = dir.join("wal.log");
        use std::io::Write as _;
        let mut f = fs::OpenOptions::new().append(true).open(&wal_path).unwrap();
        f.write_all(b"{\"collection\":\"users\",\"records\":[{\"id\":\"ghost\"}")
            .unwrap();
        f.sync_all().unwrap();
        drop(f);

        let store = Store::open(&dir).unwrap();
        let v = store.save_version().unwrap();
        let recs = store.read_records(v.id, "users").unwrap();
        let ids: Vec<_> = recs.iter().map(|r| r["id"].as_str().unwrap()).collect();
        assert_eq!(ids, vec!["u1", "u2"]);

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn replace_overwrites_records_and_reports_counts() {
        let dir = temp_dir();
        let store = Store::open(&dir).unwrap();
        store
            .write_batch(
                "users",
                &[
                    json!({"id": "u1", "name": "Ada", "age": 36}),
                    json!({"id": "u2", "name": "Lin", "age": 28}),
                ],
                false,
            )
            .unwrap();
        let v1 = store.save_version().unwrap();

        // 一批混合：1 条覆盖（内容不同）、1 条幂等（内容相同）、1 条新增。
        let out = store
            .write_batch(
                "users",
                &[
                    json!({"id": "u1", "name": "Ada", "age": 37}),
                    json!({"id": "u2", "name": "Lin", "age": 28}),
                    json!({"id": "u3", "name": "New"}),
                ],
                true,
            )
            .unwrap();
        assert_eq!(out.accepted, 3);
        assert_eq!(out.inserted, 1);
        assert_eq!(out.replaced, 1);

        // 当前数据为覆盖后的内容；字段顺序按新记录保留。
        let cur = store.inner.lock().unwrap();
        let users = cur.current.get("users").unwrap();
        assert_eq!(users.len(), 3);
        let u1 = users.iter().find(|r| r["id"] == "u1").unwrap();
        assert_eq!(
            Value::Object(u1.clone()),
            json!({"id": "u1", "name": "Ada", "age": 37})
        );
        assert_eq!(
            u1.keys().cloned().collect::<Vec<_>>(),
            vec!["id", "name", "age"]
        );
        drop(cur);

        // 已保存版本不变：仍能查到旧记录。
        let old = store.read_records(v1.id, "users").unwrap();
        let old_u1 = old.iter().find(|r| r["id"] == "u1").unwrap();
        assert_eq!(
            Value::Object(old_u1.clone()),
            json!({"id": "u1", "name": "Ada", "age": 36})
        );

        // 之后保存版本，差异中该记录出现在 changed：before 旧、after 新。
        let v2 = store.save_version().unwrap();
        let d = store.diff_versions(v1.id, v2.id, "users").unwrap();
        assert_eq!(d.changed.len(), 1);
        assert_eq!(d.changed[0].before["id"], "u1");
        assert_eq!(
            Value::Object(d.changed[0].before.clone()),
            json!({"id": "u1", "name": "Ada", "age": 36})
        );
        assert_eq!(
            Value::Object(d.changed[0].after.clone()),
            json!({"id": "u1", "name": "Ada", "age": 37})
        );

        // 字段顺序不同但内容相同：replace=true 也不计入 replaced（顺序不影响判定）。
        let out = store
            .write_batch(
                "users",
                &[json!({"age": 37, "id": "u1", "name": "Ada"})],
                true,
            )
            .unwrap();
        assert_eq!(out.inserted, 0);
        assert_eq!(out.replaced, 0);

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn replace_still_rejects_invalid_batches_atomically() {
        let dir = temp_dir();
        let store = Store::open(&dir).unwrap();
        store
            .write_batch(
                "users",
                &[json!({"id": "u1", "v": 1}), json!({"id": "u2", "v": 1})],
                false,
            )
            .unwrap();

        // 同批主键重复：即使 replace=true 也整批拒绝。
        let err = store
            .write_batch(
                "users",
                &[json!({"id": "d", "v": 1}), json!({"id": "d", "v": 2})],
                true,
            )
            .unwrap_err();
        match err {
            BatchError::Rejected(r) => assert!(r.reason.contains("duplicate id")),
            BatchError::Persist(e) => panic!("{e}"),
        }

        // 浮点字段：整批拒绝，批次中排在前面的 u1 不得被替换。
        let err = store
            .write_batch(
                "users",
                &[
                    json!({"id": "u1", "v": 2}),
                    json!({"id": "u9", "score": 1.5}),
                ],
                true,
            )
            .unwrap_err();
        match err {
            BatchError::Rejected(r) => {
                assert_eq!(r.index, 1);
                assert_eq!(r.id.as_deref(), Some("u9"));
                assert!(r.reason.contains("integer"));
            }
            BatchError::Persist(e) => panic!("{e}"),
        }

        // 数组字段：整批拒绝。
        let err = store
            .write_batch("users", &[json!({"id": "x", "tags": ["a"]})], true)
            .unwrap_err();
        match err {
            BatchError::Rejected(r) => assert!(r.reason.contains("arrays")),
            BatchError::Persist(e) => panic!("{e}"),
        }

        // 非对象记录：整批拒绝。
        let err = store
            .write_batch("users", &[json!([1, 2])], true)
            .unwrap_err();
        match err {
            BatchError::Rejected(r) => assert!(r.reason.contains("JSON object")),
            BatchError::Persist(e) => panic!("{e}"),
        }

        // 缺主键：整批拒绝。
        let err = store
            .write_batch("users", &[json!({"v": 2})], true)
            .unwrap_err();
        match err {
            BatchError::Rejected(r) => assert!(r.reason.contains("missing primary key")),
            BatchError::Persist(e) => panic!("{e}"),
        }

        // 全部未生效：u1/u2 保持原值，没有新记录，replaced 不残留。
        let cur = store.inner.lock().unwrap();
        let users = cur.current.get("users").unwrap();
        assert_eq!(users.len(), 2);
        assert_eq!(users.iter().find(|r| r["id"] == "u1").unwrap()["v"], 1);
        drop(cur);

        // replace=false 的旧规则不变：同主键不同内容仍拒绝。
        let err = store
            .write_batch("users", &[json!({"id": "u1", "v": 2})], false)
            .unwrap_err();
        match err {
            BatchError::Rejected(r) => assert!(r.reason.contains("different record")),
            BatchError::Persist(e) => panic!("{e}"),
        }

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn replacements_survive_restart_and_keep_snapshots() {
        let dir = temp_dir();
        let v1_id;
        {
            let store = Store::open(&dir).unwrap();
            store
                .write_batch("users", &[json!({"id": "u1", "v": 1})], false)
                .unwrap();
            v1_id = store.save_version().unwrap().id;
            store
                .write_batch(
                    "users",
                    &[json!({"id": "u1", "v": 2}), json!({"id": "u2", "v": 9})],
                    true,
                )
                .unwrap();
        }
        // 重启：已确认的替换通过 WAL 重放生效，已保存版本不变。
        let store = Store::open(&dir).unwrap();
        let old = store.read_records(v1_id, "users").unwrap();
        assert_eq!(old.len(), 1);
        assert_eq!(old[0]["v"], 1);
        let v2 = store.save_version().unwrap();
        let recs = store.read_records(v2.id, "users").unwrap();
        let by_id = |id: &str| Value::Object(recs.iter().find(|r| r["id"] == id).unwrap().clone());
        assert_eq!(by_id("u1"), json!({"id": "u1", "v": 2}));
        assert_eq!(by_id("u2"), json!({"id": "u2", "v": 9}));

        fs::remove_dir_all(&dir).ok();
    }
}
