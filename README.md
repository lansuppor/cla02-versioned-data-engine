# Versioned Data Engine

面向团队数据服务的 Rust HTTP/JSON 程序。提供健康状态、版本信息，数据集创建、记录写入、按标识读取、记录删除，以及**数据版本保存、按版本读取与版本比较**能力（当前为内存存储，进程退出后数据不保留）。

需要 Rust 1.98.1；工具链由 `rust-toolchain.toml` 固定。

```sh
cargo build --locked
VDE_BIND=127.0.0.1:8080 cargo run --locked
```

`VDE_BIND` 可指定监听地址，默认 `127.0.0.1:8080`。按 Ctrl-C 停止服务。

## 接口

| 接口 | 说明 | 响应 |
| --- | --- | --- |
| `GET /health` | 健康检查 | `{"status":"ok"}` |
| `GET /version` | 版本信息 | `{"name":"versioned-data-engine","version":"0.1.0"}` |
| `POST /datasets` | 创建数据集，请求体 `{"name":"..."}`；名称唯一、非空且不含空白字符 | 成功 `201 {"name":"..."}`；重名 `409`；名称非法 `400` |
| `PUT /datasets/{dataset}/records` | 写入记录，请求体为 JSON 对象且必须含字符串字段 `"id"`；同标识重复写入整体替换 | 成功 `200 {"dataset":"...","id":"..."}`；数据集不存在 `404`；请求体非法 `400` |
| `GET /datasets/{dataset}/records/{id}` | 按标识读取**当前最新**记录（不反映历史版本） | 成功 `200 {"dataset","id","record"}`；数据集或记录不存在 `404` |
| `DELETE /datasets/{dataset}/records/{id}` | 删除当前记录；已保存版本中的副本不受影响 | 成功 `200 {"dataset","id"}`；数据集或记录不存在 `404` |
| `POST /datasets/{dataset}/versions` | 为数据集当时的全部记录保存不可变快照，无需请求体 | `201 {"dataset":"...","version":"vN"}`；版本标识由服务生成、数据集内唯一；内容完全相同的两次保存也各自独立；数据集不存在 `404` |
| `GET /datasets/{dataset}/versions/{version}/records` | 按版本读取整批记录（保存那一刻的快照） | `200 {"dataset","version","records":{id: record}}`；数据集或版本不存在 `404`，不创建占位数据 |
| `GET /datasets/{dataset}/versions/{version}/records/{id}` | 按版本读取单条记录 | `200 {"dataset","version","id","record"}`；数据集、版本或该版本内记录不存在 `404` |
| `POST /datasets/{dataset}/versions/compare` | 比较两个版本，请求体 `{"from":"v1","to":"v2"}`，顺序由调用方指定 | `200 {"dataset","from","to","added":[…],"removed":[…],"changed":[…]}`；任一引用版本或数据集不存在则整体 `404`，不产生部分结果；请求体非法 `400` |

比较语义（按记录标识）：

- `added`：仅存在于 `to`（后一个版本）的记录，元素为 `{"id","record"}`；
- `removed`：仅存在于 `from`（前一个版本）的记录，元素为 `{"id","record"}`；
- `changed`：两边都存在但内容不同，元素为 `{"id","before","after"}`；
- 内容完全一致时三个数组均为空；交换 `from`/`to` 则新增与删除互换，`before`/`after` 也随之互换。

版本快照不可变：保存成功后对记录的新增、整体替换或删除都不会改变任何已保存版本的内容。版本保存在服务端单个互斥锁内原子完成——要么产生可查询的版本，要么不留任何版本。

错误响应统一为 `{"error":"原因说明"}`。失败的请求不会留下任何部分数据。

## 示例

```sh
curl http://127.0.0.1:8080/health
curl http://127.0.0.1:8080/version

# 创建数据集并写入记录（支持嵌套对象与数组）
curl -X POST http://127.0.0.1:8080/datasets -d '{"name":"users"}'
curl -X PUT http://127.0.0.1:8080/datasets/users/records \
  -d '{"id":"u1","name":"Ada","tags":["a","b"],"meta":{"age":36}}'
curl -X PUT http://127.0.0.1:8080/datasets/users/records \
  -d '{"id":"u2","name":"Bob"}'

# 按标识读取当前数据
curl http://127.0.0.1:8080/datasets/users/records/u1

# 保存一次数据版本，得到版本标识（如 v1）
curl -X POST http://127.0.0.1:8080/datasets/users/versions

# 此后覆盖 / 删除 / 新增记录，不影响已保存的 v1
curl -X PUT http://127.0.0.1:8080/datasets/users/records -d '{"id":"u1","name":"Ada v2"}'
curl -X DELETE http://127.0.0.1:8080/datasets/users/records/u2
curl -X PUT http://127.0.0.1:8080/datasets/users/records -d '{"id":"u3","name":"Cyd"}'

# 再保存一个版本
curl -X POST http://127.0.0.1:8080/datasets/users/versions   # -> v2

# 按版本读取：单条或整批（还原保存那一刻的集合）
curl http://127.0.0.1:8080/datasets/users/versions/v1/records/u1
curl http://127.0.0.1:8080/datasets/users/versions/v1/records

# 比较两个版本（from -> to，顺序影响新增/删除方向）
curl -X POST http://127.0.0.1:8080/datasets/users/versions/compare \
  -d '{"from":"v1","to":"v2"}'
# 反向比较
curl -X POST http://127.0.0.1:8080/datasets/users/versions/compare \
  -d '{"from":"v2","to":"v1"}'
```

## 测试

```sh
cargo test --locked
```
