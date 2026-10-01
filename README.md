# Versioned Data Engine

面向团队数据服务的 Rust HTTP/JSON 程序。当前提供健康状态、版本信息，以及版本化数据能力的第一步：创建具名数据集、向数据集写入结构化记录、按标识读取单条记录。后续将在此基础上扩展版本化查询。

需要 Rust 1.98.1；工具链由 `rust-toolchain.toml` 固定。

```sh
cargo build --locked
VDE_BIND=127.0.0.1:8080 cargo run --locked
```

`VDE_BIND` 可指定监听地址，默认 `127.0.0.1:8080`。按 Ctrl-C 停止服务。
数据保存在进程内存中，重启后清空。

## 基础接口

| 接口 | 响应 |
| --- | --- |
| `GET /health` | `{"status":"ok"}` |
| `GET /version` | `{"name":"versioned-data-engine","version":"0.1.0"}` |

## 数据集与记录接口

- `PUT /datasets/{name}`：创建具名数据集。名称在服务内唯一，非空且不能包含空白字符。
  重名创建返回 `409`，不会覆盖或清空已有数据。
- `PUT /datasets/{name}/records`：写入一条记录（请求体为 JSON 对象，需带字符串类型的 `id`
  字段，标识在数据集内唯一）。同一 `id` 重复写入时整体替换旧记录，其余记录不受影响；
  连续多次替换按到达顺序生效，读取结果始终为最后一次成功写入的内容。
  向不存在的数据集写入返回 `404`，不会隐式创建数据集。
- `GET /datasets/{name}/records/{id}`：按标识读取记录，返回记录当前内容及所属数据集。
  数据集或标识不存在时返回 `404`，不会创建空数据集或占位记录。

写入成功（`201` 新建 / `200` 替换）后立即可被后续读取观察到。非法 JSON、缺少 `id`、
`id` 类型错误等请求一律返回 `400` 并说明原因，不写入任何部分数据。

```sh
# 健康与版本
curl http://127.0.0.1:8080/health
curl http://127.0.0.1:8080/version

# 1. 创建数据集
curl -i -X PUT http://127.0.0.1:8080/datasets/users
#   201 {"name":"users"}；重复创建返回 409

# 2. 写入记录（支持嵌套对象与数组，id 必须是字符串）
curl -i -X PUT http://127.0.0.1:8080/datasets/users/records \
  -H 'Content-Type: application/json' \
  -d '{"id":"u1","name":"Ada","tags":["a","b"],"addr":{"city":"X"}}'
#   201 新建；同一 id 再次写入返回 200 并整体替换

# 3. 按标识读取
curl -i http://127.0.0.1:8080/datasets/users/records/u1
#   200 {"dataset":"users","id":"u1","record":{...}}

# 错误示例：非法 JSON / 缺 id / id 非字符串 -> 400
curl -i -X PUT http://127.0.0.1:8080/datasets/users/records \
  -H 'Content-Type: application/json' -d '{not json'
# 向不存在的数据集写入 -> 404（不自动创建）
curl -i -X PUT http://127.0.0.1:8080/datasets/ghost/records \
  -H 'Content-Type: application/json' -d '{"id":"x"}'
# 读取不存在的数据集或标识 -> 404
curl -i http://127.0.0.1:8080/datasets/users/records/nope
```
