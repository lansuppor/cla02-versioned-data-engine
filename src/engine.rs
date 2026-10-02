//! 版本化数据存储引擎：集合写入校验、只读快照与磁盘持久化。

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// 字段类型。集合内所有记录的结构必须一致，嵌套对象递归比较。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FieldType {
    String,
    Integer,
    Boolean,
    Null,
    Object(BTreeMap<String, FieldType>),
}

impl FieldType {
    fn name(&self) -> &'static str {
        match self {
            FieldType::String => "string",
            FieldType::Integer => "integer",
            FieldType::Boolean => "boolean",
            FieldType::Null => "null",
            FieldType::Object(_) => "object",
        }
    }
}

type Schema = BTreeMap<String, FieldType>;

struct Collection {
    schema: Schema,
    records: Vec<Value>,
    index: HashMap<String, usize>,
}

#[derive(Serialize, Deserialize)]
struct CollectionFile {
    schema: Schema,
    records: Vec<Value>,
}

/// 只读快照：保存时刻各集合的全部记录。
#[derive(Clone, Serialize, Deserialize)]
pub struct Version {
    pub id: String,
    pub saved_at: String,
    pub collections: BTreeMap<String, Vec<Value>>,
}

#[derive(Debug)]
pub enum WriteError {
    Invalid(String),
    Io(io::Error),
}

fn invalid(message: String) -> WriteError {
    WriteError::Invalid(message)
}

pub struct Engine {
    dir: PathBuf,
    collections: BTreeMap<String, Collection>,
    versions: BTreeMap<String, Version>,
    next_version: u64,
}

impl Engine {
    /// 从数据目录恢复已持久化的集合与版本；目录不存在时从空状态开始。
    pub fn load(dir: PathBuf) -> io::Result<Engine> {
        let mut engine = Engine {
            dir,
            collections: BTreeMap::new(),
            versions: BTreeMap::new(),
            next_version: 1,
        };

        let collections_dir = engine.collections_dir();
        if collections_dir.is_dir() {
            for entry in fs::read_dir(&collections_dir)? {
                let path = entry?.path();
                if path.extension().and_then(|e| e.to_str()) != Some("json") {
                    continue;
                }
                let file: CollectionFile = serde_json::from_slice(&fs::read(&path)?)
                    .map_err(|e| invalid_data(&path, e))?;
                let name = path.file_stem().unwrap().to_string_lossy().into_owned();
                let mut index = HashMap::new();
                for (pos, record) in file.records.iter().enumerate() {
                    if let Some(id) = record.get("id").and_then(Value::as_str) {
                        index.insert(id.to_owned(), pos);
                    }
                }
                engine.collections.insert(
                    name,
                    Collection {
                        schema: file.schema,
                        records: file.records,
                        index,
                    },
                );
            }
        }

        let versions_dir = engine.versions_dir();
        if versions_dir.is_dir() {
            for entry in fs::read_dir(&versions_dir)? {
                let path = entry?.path();
                if path.extension().and_then(|e| e.to_str()) != Some("json") {
                    continue;
                }
                let version: Version = serde_json::from_slice(&fs::read(&path)?)
                    .map_err(|e| invalid_data(&path, e))?;
                if let Some(n) = version
                    .id
                    .strip_prefix('v')
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    engine.next_version = engine.next_version.max(n + 1);
                }
                engine.versions.insert(version.id.clone(), version);
            }
        }

        Ok(engine)
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn collections_dir(&self) -> PathBuf {
        self.dir.join("collections")
    }

    fn versions_dir(&self) -> PathBuf {
        self.dir.join("versions")
    }

    /// 校验并提交一批记录。任何一条不合格则整批不生效；先落盘成功才更新内存。
    pub fn write_batch(&mut self, name: &str, records: Vec<Value>) -> Result<usize, WriteError> {
        if records.is_empty() {
            return Err(invalid("records must be a non-empty array".to_owned()));
        }

        let mut seen = HashSet::new();
        for (i, record) in records.iter().enumerate() {
            let n = i + 1;
            let obj = record
                .as_object()
                .ok_or_else(|| invalid(format!("record {n}: must be a JSON object")))?;
            let id = obj
                .get("id")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    invalid(format!(
                        "record {n}: primary key 'id' must be a non-empty string"
                    ))
                })?;
            if !seen.insert(id.to_owned()) {
                return Err(invalid(format!(
                    "record {n}: duplicate primary key \"{id}\" in batch"
                )));
            }
            for (key, value) in obj {
                validate_value(value, key)
                    .map_err(|m| invalid(format!("record {n}: {m}")))?;
            }
        }

        let expected = match self.collections.get(name) {
            Some(collection) => collection.schema.clone(),
            None => record_schema(records[0].as_object().unwrap()),
        };
        for (i, record) in records.iter().enumerate() {
            check_schema(&expected, record.as_object().unwrap(), "")
                .map_err(|m| invalid(format!("record {}: {m}", i + 1)))?;
        }

        let (mut new_records, mut new_index) = match self.collections.get(name) {
            Some(collection) => (collection.records.clone(), collection.index.clone()),
            None => (Vec::new(), HashMap::new()),
        };
        for record in &records {
            let id = record["id"].as_str().unwrap().to_owned();
            match new_index.get(&id) {
                Some(&pos) => new_records[pos] = record.clone(),
                None => {
                    new_index.insert(id, new_records.len());
                    new_records.push(record.clone());
                }
            }
        }

        let file = CollectionFile {
            schema: expected.clone(),
            records: new_records.clone(),
        };
        let data =
            serde_json::to_vec_pretty(&file).map_err(|e| WriteError::Io(io::Error::other(e)))?;
        write_atomic(
            &self.collections_dir().join(format!("{name}.json")),
            &data,
        )
        .map_err(WriteError::Io)?;

        self.collections.insert(
            name.to_owned(),
            Collection {
                schema: expected,
                records: new_records,
                index: new_index,
            },
        );
        Ok(records.len())
    }

    /// 把当前各集合固化为只读快照，返回版本标识与保存时间。
    pub fn save_version(&mut self) -> io::Result<Version> {
        let version = Version {
            id: format!("v{}", self.next_version),
            saved_at: now_rfc3339(),
            collections: self
                .collections
                .iter()
                .map(|(name, c)| (name.clone(), c.records.clone()))
                .collect(),
        };
        let data = serde_json::to_vec_pretty(&version).map_err(io::Error::other)?;
        write_atomic(
            &self.versions_dir().join(format!("{}.json", version.id)),
            &data,
        )?;
        self.next_version += 1;
        self.versions.insert(version.id.clone(), version.clone());
        Ok(version)
    }

    pub fn version(&self, id: &str) -> Option<&Version> {
        self.versions.get(id)
    }
}

/// 校验单个字段值，返回其类型；不支持的类型（数组、浮点数）报错并指明字段路径。
fn validate_value(value: &Value, path: &str) -> Result<FieldType, String> {
    match value {
        Value::Null => Ok(FieldType::Null),
        Value::Bool(_) => Ok(FieldType::Boolean),
        Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                Ok(FieldType::Integer)
            } else {
                Err(format!("field '{path}': only integer numbers are allowed"))
            }
        }
        Value::String(_) => Ok(FieldType::String),
        Value::Array(_) => Err(format!("field '{path}': arrays are not supported")),
        Value::Object(map) => {
            let mut fields = BTreeMap::new();
            for (key, value) in map {
                fields.insert(key.clone(), validate_value(value, &format!("{path}.{key}"))?);
            }
            Ok(FieldType::Object(fields))
        }
    }
}

fn record_schema(obj: &Map<String, Value>) -> Schema {
    obj.iter()
        .map(|(key, value)| (key.clone(), validate_value(value, key).unwrap()))
        .collect()
}

/// 对照集合格式检查记录结构：字段缺失、字段多余或类型不符均报错。
fn check_schema(schema: &Schema, obj: &Map<String, Value>, prefix: &str) -> Result<(), String> {
    for key in schema.keys() {
        if !obj.contains_key(key) {
            return Err(format!("field '{prefix}{key}': missing"));
        }
    }
    for (key, value) in obj {
        let path = format!("{prefix}{key}");
        let Some(expected) = schema.get(key) else {
            return Err(format!("field '{path}': not allowed by collection schema"));
        };
        let actual = validate_value(value, &path)?;
        match (expected, &actual) {
            (FieldType::Object(exp), FieldType::Object(_)) => {
                check_schema(exp, value.as_object().unwrap(), &format!("{path}."))?;
            }
            _ if *expected == actual => {}
            _ => {
                return Err(format!(
                    "field '{path}': expected {}, got {}",
                    expected.name(),
                    actual.name()
                ));
            }
        }
    }
    Ok(())
}

/// 先写临时文件并 fsync，再原子改名；崩溃时只会留下不可见的临时文件。
fn write_atomic(path: &Path, data: &[u8]) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("json.tmp");
    let mut file = fs::File::create(&tmp)?;
    file.write_all(data)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&tmp, path)?;
    Ok(())
}

fn invalid_data(path: &Path, error: serde_json::Error) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("{}: {error}", path.display()),
    )
}

fn now_rfc3339() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format_rfc3339(secs)
}

fn format_rfc3339(secs: u64) -> String {
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let (hh, mm, ss) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    // 天数转公历日期（Howard Hinnant 算法）
    let z = days + 719_468;
    let era = z / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { year + 1 } else { year };
    format!("{year:04}-{month:02}-{day:02}T{hh:02}:{mm:02}:{ss:02}Z")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn temp_dir() -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "vde-test-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    fn write(engine: &mut Engine, name: &str, records: Vec<Value>) -> Result<usize, WriteError> {
        engine.write_batch(name, records)
    }

    #[test]
    fn rejects_invalid_batch_without_side_effects() {
        let mut engine = Engine::load(temp_dir()).unwrap();
        write(&mut engine, "users", vec![json!({"id": "a", "age": 30})]).unwrap();

        // 缺主键
        let err = write(&mut engine, "users", vec![json!({"age": 31})]);
        assert!(matches!(err, Err(WriteError::Invalid(m)) if m.contains("record 1: primary key")));

        // 同批主键重复
        let err = write(
            &mut engine,
            "users",
            vec![json!({"id": "b", "age": 1}), json!({"id": "b", "age": 2})],
        );
        assert!(matches!(err, Err(WriteError::Invalid(m)) if m.contains("record 2: duplicate primary key")));

        // 浮点数与数组不支持
        let err = write(&mut engine, "users", vec![json!({"id": "c", "age": 1.5})]);
        assert!(matches!(err, Err(WriteError::Invalid(m)) if m.contains("record 1: field 'age': only integer")));
        let err = write(&mut engine, "users", vec![json!({"id": "c", "age": [1]})]);
        assert!(matches!(err, Err(WriteError::Invalid(m)) if m.contains("record 1: field 'age': arrays are not supported")));

        // 结构不一致：类型不符、字段缺失、字段多余
        let err = write(&mut engine, "users", vec![json!({"id": "c", "age": "x"})]);
        assert!(matches!(err, Err(WriteError::Invalid(m)) if m.contains("record 1: field 'age': expected integer, got string")));
        let err = write(&mut engine, "users", vec![json!({"id": "c"})]);
        assert!(matches!(err, Err(WriteError::Invalid(m)) if m.contains("record 1: field 'age': missing")));
        let err = write(&mut engine, "users", vec![json!({"id": "c", "age": 1, "extra": true})]);
        assert!(matches!(err, Err(WriteError::Invalid(m)) if m.contains("record 1: field 'extra': not allowed")));

        // 全部失败后数据保持原样
        let version = engine.save_version().unwrap();
        assert_eq!(version.collections["users"], vec![json!({"id": "a", "age": 30})]);
    }

    #[test]
    fn upserts_and_validates_nested_objects() {
        let mut engine = Engine::load(temp_dir()).unwrap();
        write(
            &mut engine,
            "users",
            vec![json!({"id": "a", "meta": {"active": true, "note": null}})],
        )
        .unwrap();
        // 嵌套结构不一致
        let err = write(
            &mut engine,
            "users",
            vec![json!({"id": "b", "meta": {"active": true}})],
        );
        assert!(matches!(err, Err(WriteError::Invalid(m)) if m.contains("record 1: field 'meta.note': missing")));
        // 同主键跨批覆盖
        write(
            &mut engine,
            "users",
            vec![json!({"id": "a", "meta": {"active": false, "note": null}})],
        )
        .unwrap();
        let version = engine.save_version().unwrap();
        assert_eq!(
            version.collections["users"],
            vec![json!({"id": "a", "meta": {"active": false, "note": null}})]
        );
    }

    #[test]
    fn snapshot_is_immutable() {
        let mut engine = Engine::load(temp_dir()).unwrap();
        write(&mut engine, "users", vec![json!({"id": "a", "age": 30})]).unwrap();
        let v1 = engine.save_version().unwrap();
        write(&mut engine, "users", vec![json!({"id": "a", "age": 31})]).unwrap();
        write(&mut engine, "teams", vec![json!({"id": "t1"})]).unwrap();
        let v2 = engine.save_version().unwrap();

        assert_eq!(v1.id, "v1");
        assert_eq!(v2.id, "v2");
        let v1 = engine.version("v1").unwrap();
        assert_eq!(v1.collections["users"], vec![json!({"id": "a", "age": 30})]);
        assert!(!v1.collections.contains_key("teams"));
        let v2 = engine.version("v2").unwrap();
        assert_eq!(v2.collections["users"], vec![json!({"id": "a", "age": 31})]);
    }

    #[test]
    fn persists_across_restart() {
        let dir = temp_dir();
        let mut engine = Engine::load(dir.clone()).unwrap();
        write(&mut engine, "users", vec![json!({"id": "a", "age": 30})]).unwrap();
        engine.save_version().unwrap();
        write(&mut engine, "users", vec![json!({"id": "b", "age": 20})]).unwrap();
        engine.save_version().unwrap();
        drop(engine);

        let engine = Engine::load(dir.clone()).unwrap();
        let v1 = engine.version("v1").unwrap();
        assert_eq!(v1.collections["users"], vec![json!({"id": "a", "age": 30})]);
        let v2 = engine.version("v2").unwrap();
        assert_eq!(
            v2.collections["users"],
            vec![json!({"id": "a", "age": 30}), json!({"id": "b", "age": 20})]
        );
        // 版本号继续递增，不与已有版本冲突
        let mut engine = engine;
        assert_eq!(engine.save_version().unwrap().id, "v3");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn formats_rfc3339() {
        assert_eq!(format_rfc3339(0), "1970-01-01T00:00:00Z");
        assert_eq!(format_rfc3339(1_700_000_000), "2023-11-14T22:13:20Z");
    }
}
