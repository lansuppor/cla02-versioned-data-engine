# Versioned Data Engine

面向团队数据服务的 Rust HTTP/JSON 程序。提供健康状态、版本信息，数据集创建、记录写入、按标识读取、记录删除，以及**数据版本保存、按版本读取与版本比较**能力（当前为内存存储，进程退出后数据不保留）。

需要 Rust 1.98.1；工具链由 `rust-toolchain.toml` 固定。

```sh
cargo build --locked
VDE_BIND=127.0.0.1:8080 cargo run --locked
```

`VDE_BIND` 可指定监听地址，默认 `127.0.0.1:8080`。按 Ctrl-C 停止服务。

## 接口

### 基础与当前数据

| 接口 | 说明 | 响应 |
| --- | --- | --- |
| `GET /health` | 健康检查 | `{"status":"ok"}` |
| `GET /version` | 版本信息 | `{"name":"versioned-data-engine","version":"0.1.0"}` |
| `POST /datasets` | 创建数据集，请求体 `{"name":"..."}`；名称唯一、非空且不含空白字符 | 成功 `201 {"name":"..."}`；重名 `409`；名称非法 `400` |
| `PUT /datasets/{dataset}/records` | 写入记录，请求体为 JSON 对象且必须含字符串字段 `"id"`；同标识重复写入整体替换 | 成功 `200 {"dataset":"...","id":"..."}`；数据集不存在 `404`；请求体非法 `400` |
| `GET /datasets/{dataset}/records/{id}` | 按标识读取**当前最新**记录（不反映历史版本） | 成功 `200 {"dataset":"...","id":"...","record":{...}}`；数据集或记录不存在 `404` |
| `DELETE /datasets/{dataset}/records/{id}` | 删除当前数据中的记录；已保存版本中的内容不受影响 | 成功 `200 {"dataset":"...","id":"..."}`；数据集或记录不存在 `404` |

### 数据版本

| 接口 | 说明 | 响应 |
| --- | --- | --- |
| `POST /datasets/{dataset}/versions` | 为数据集当时的全部记录建立不可变快照，服务生成数据集内唯一的版本标识；即使内容与上次完全一致，也返回独立的新版本标识；保存是原子的 | 成功 `201 {"dataset":"...","version":"v1","record_count":N}`；数据集不存在 `404` |
| `GET /datasets/{dataset}/versions/{version}/records` | 按版本读取整批记录，还原保存那一刻的记录集合（按记录 id 排序）；引用不存在的数据集或版本返回 `404`，不会创建空版本 | 成功 `200 {"dataset":"...","version":"...","records":[...]}` |
| `GET /datasets/{dataset}/versions/{version}/records/{id}` | 按版本读取单条记录 | 成功 `200 {"dataset":"...","version":"...","id":"...","record":{...}}`；数据集、版本或版本内记录不存在 `404` |
| `POST /datasets/{dataset}/versions/compare` | 比较两个版本，请求体 `{"from":"前一版本","to":"后一版本"}`，前后顺序由调用方指定 | 成功 `200`（见下）；任一引用的版本或数据集不存在整体 `404` 并说明原因，不产生部分结果；请求体非法 `400` |

比较成功响应：

```json
{
  "dataset": "users",
  "from": "v1",
  "to": "v2",
  "added":   [{ "id": "b", "record": { ... } }],
  "removed": [{ "id": "a", "record": { ... } }],
  "changed": [{ "id": "c", "before": { ... }, "after": { ... } }]
}
```

- `added`：仅存在于 `to`（后一个版本）的记录；
- `removed`：仅存在于 `from`（前一个版本）的记录；
- `changed`：两个版本都存在但内容不同的记录，给出 `before`/`after`；
- 两个版本内容完全一致（包括版本与自身比较）时，三个列表均为空；
- 同一对版本按相反顺序比较时，新增与删除互换，`before`/`after` 也随之互换。

错误响应统一为 `{"error":"原因说明"}`。失败的请求不会留下任何部分数据。

## 示例

```sh
curl http://127.0.0.1:8080/health
curl http://127.0.0.1:8080/version

# 创建数据集并写入两条记录
curl -X POST http://127.0.0.1:8080/datasets -d '{"name":"users"}'
curl -X PUT http://127.0.0.1:8080/datasets/users/records \
  -d '{"id":"u1","name":"Ada","tags":["a","b"],"meta":{"age":36}}'
curl -X PUT http://127.0.0.1:8080/datasets/users/records \
  -d '{"id":"u2","name":"Lin"}'

# 按标识读取当前数据
curl http://127.0.0.1:8080/datasets/users/records/u1

# 保存版本 v1（版本标识由服务生成并返回）
curl -X POST http://127.0.0.1:8080/datasets/users/versions
# -> {"dataset":"users","version":"v1","record_count":2}

# 之后修改当前数据：覆盖 u2、删除 u1、新增 u3
curl -X PUT http://127.0.0.1:8080/datasets/users/records -d '{"id":"u2","name":"Linus"}'
curl -X DELETE http://127.0.0.1:8080/datasets/users/records/u1
curl -X PUT http://127.0.0.1:8080/datasets/users/records -d '{"id":"u3","name":"Joy"}'

# 再保存版本 v2
curl -X POST http://127.0.0.1:8080/datasets/users/versions
# -> {"dataset":"users","version":"v2","record_count":2}

# 按版本读取：v1 仍是保存那一刻的两条记录（已删除的 u1 仍可读）
curl http://127.0.0.1:8080/datasets/users/versions/v1/records
curl http://127.0.0.1:8080/datasets/users/versions/v1/records/u1

# 比较 v1 -> v2：u1 删除、u3 新增、u2 内容变更
curl -X POST http://127.0.0.1:8080/datasets/users/versions/compare \
  -d '{"from":"v1","to":"v2"}'

# 反向比较：新增与删除互换、before/after 互换
curl -X POST http://127.0.0.1:8080/datasets/users/versions/compare \
  -d '{"from":"v2","to":"v1"}'

# 读取不存在的版本 -> 404 {"error":"..."}，不会创建空版本
curl -i http://127.0.0.1:8080/datasets/users/versions/v9/records
```

## 测试

```sh
cargo test --locked
```
