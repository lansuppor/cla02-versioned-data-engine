# Versioned Data Engine

面向团队数据服务的 Rust HTTP/JSON 程序。支持向集合写入结构化记录、保存只读数据版本，并查询任一历史版本的数据视图。

需要 Rust 1.98.1；工具链由 `rust-toolchain.toml` 固定。

```sh
cargo build --locked
VDE_BIND=127.0.0.1:8080 VDE_DATA_DIR=./data cargo run --locked
```

`VDE_BIND` 指定监听地址，默认 `127.0.0.1:8080`；`VDE_DATA_DIR` 指定数据目录，默认 `./data`。集合与版本以 JSON 文件原子写入该目录，进程重启后自动恢复；崩溃时未完成的写入只残留不可见的临时文件。按 Ctrl-C 停止服务。

## 接口

| 接口 | 说明 |
| --- | --- |
| `GET /health` | `{"status":"ok"}` |
| `GET /version` | `{"name":"versioned-data-engine","version":"0.1.0"}` |
| `POST /collections/{collection}/records` | 批量写入记录，全有或全无 |
| `POST /versions` | 保存只读快照，返回版本标识与保存时间 |
| `GET /versions/{version}/collections/{collection}/records` | 查询指定版本中指定集合的全部记录 |

### 写入记录

`POST /collections/{collection}/records`，请求体 `{"records": [...]}`。集合在首次写入时创建，集合名限 64 个以内的字母、数字、`-`、`_`。

记录是 JSON 对象，必须带非空字符串主键 `id`；字段值只能是字符串、整数、布尔值、空值或嵌套对象（不支持数组和浮点数）。同一集合内所有记录结构（字段名与类型，含嵌套）必须一致；同一主键跨批再次写入会覆盖该记录。任何一条记录不合格（缺主键、同批主键重复、类型不符、结构与集合不一致）则整批拒绝，已有数据不变，错误信息指明是哪条记录的哪个问题。

```sh
curl -X POST http://127.0.0.1:8080/collections/users/records \
  -H 'Content-Type: application/json' \
  -d '{"records":[{"id":"u1","name":"Alice","age":30,"active":true,"note":null,"meta":{"team":"core","level":2}}]}'
# => {"collection":"users","written":1}

curl -X POST http://127.0.0.1:8080/collections/users/records \
  -H 'Content-Type: application/json' \
  -d '{"records":[{"id":"u2","name":"Bob","age":"thirty","active":true,"note":null,"meta":{"team":"edge","level":1}}]}'
# => 400 {"error":"record 1: field 'age': expected integer, got string"}
```

### 保存版本

`POST /versions` 把当前各集合固化为只读快照，返回可重复使用的版本标识和保存时间（RFC 3339，UTC）。快照不受后续写入影响；与写入并发进行时，快照完整对应该次保存之前已提交的全部批次，不会出现半批记录。

```sh
curl -X POST http://127.0.0.1:8080/versions
# => {"version":"v1","saved_at":"2026-10-02T06:56:48Z"}
```

### 查询版本

`GET /versions/{version}/collections/{collection}/records` 返回该版本内指定集合的全部记录，字段顺序和值与写入时一致。版本或集合不存在时返回 404，不影响任何已有数据。

```sh
curl http://127.0.0.1:8080/versions/v1/collections/users/records
# => {"version":"v1","collection":"users","records":[{"id":"u1","name":"Alice","age":30,"active":true,"note":null,"meta":{"team":"core","level":2}}]}

curl http://127.0.0.1:8080/versions/v9/collections/users/records
# => 404 {"error":"version 'v9' not found"}
```
