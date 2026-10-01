# Versioned Data Engine

面向团队数据服务的 Rust HTTP/JSON 程序。提供健康状态、版本信息，以及数据集创建、记录写入与按标识读取能力（当前为内存存储，进程退出后数据不保留）。

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
| `GET /datasets/{dataset}/records/{id}` | 按标识读取记录 | 成功 `200 {"dataset":"...","id":"...","record":{...}}`；数据集或记录不存在 `404` |

错误响应统一为 `{"error":"原因说明"}`。失败的请求不会留下任何部分数据。

## 示例

```sh
curl http://127.0.0.1:8080/health
curl http://127.0.0.1:8080/version

# 创建数据集
curl -X POST http://127.0.0.1:8080/datasets -d '{"name":"users"}'

# 写入记录（支持嵌套对象与数组）
curl -X PUT http://127.0.0.1:8080/datasets/users/records \
  -d '{"id":"u1","name":"Ada","tags":["a","b"],"meta":{"age":36}}'

# 按标识读取
curl http://127.0.0.1:8080/datasets/users/records/u1
```

## 测试

```sh
cargo test --locked
```
