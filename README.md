# Versioned Data Engine

面向团队数据服务的 Rust HTTP/JSON 程序。支持向集合批量写入结构化记录、把某一时刻的数据
保存为可识别的只读版本，并随时查询任一历史版本的数据视图。

需要 Rust 1.98.1；工具链由 `rust-toolchain.toml` 固定。

```sh
cargo build --locked
VDE_BIND=127.0.0.1:8080 VDE_DATA=./data cargo run --locked
```

- `VDE_BIND` 指定监听地址，默认 `127.0.0.1:8080`。
- `VDE_DATA` 指定数据目录（快照与 WAL），默认 `./data`，首次写入时自动创建。
- 按 Ctrl-C 停止服务。数据持久化在数据目录中，重启后已保存版本与已确认写入均可恢复。

## 基础接口（行为保持不变）

| 接口 | 响应 |
| --- | --- |
| `GET /health` | `{"status":"ok"}` |
| `GET /version` | `{"name":"versioned-data-engine","version":"0.1.0"}` |

## 数据接口

| 接口 | 方法 | 说明 |
| --- | --- | --- |
| `/collections/{集合名}/records` | POST | 提交一批记录，全有或全无；`"replace":true` 时允许替换同主键记录，`"delete":[...]` 时按主键删除；集合首次写入时自动创建 |
| `/versions` | POST | 把当前各集合数据固化为只读版本，返回版本标识与保存时间 |
| `/versions` | GET | 列出全部已保存版本 |
| `/versions/{id}` | GET | 查询单个版本的详情（含集合名列表） |
| `/versions/{id}/collections/{集合名}/records` | GET | 查询某版本内某集合的全部记录 |
| `/versions/{起始版本}/collections/{集合名}/diff/{目标版本}` | GET | 比较同一集合在两个已保存版本之间的差异 |

记录是带唯一字符串主键 `id` 的 JSON 对象；字段值可以是字符串、整数、布尔值、`null` 或
嵌套对象（不允许浮点数与数组）。返回的记录保持写入时的字段顺序。

### 写入一批记录

```sh
curl -s -X POST http://127.0.0.1:8080/collections/users/records \
  -H 'Content-Type: application/json' \
  -d '{
    "records": [
      {"id": "u1", "name": "Ada", "age": 36, "active": true,  "meta": {"role": "admin"}},
      {"id": "u2", "name": "Lin", "age": 28, "active": false, "meta": null}
    ]
  }'
# {"collection":"users","accepted":2,"inserted":2}
```

整批要么全部生效、要么全部不生效。出现以下任一情况时整批拒绝（HTTP 400），并指出是
批次内第几条（`index`，从 0 开始）记录的什么问题（`error`）及主键（`id`）：

- 记录不是 JSON 对象，或缺失/非法的字符串主键 `id`；
- 同一批内主键重复；
- 字段类型不合规（浮点数、数组等）；
- 集合中已存在同一主键但内容不同的记录。

```sh
curl -s -X POST http://127.0.0.1:8080/collections/users/records \
  -H 'Content-Type: application/json' \
  -d '{"records":[{"id":"u9","ok":true},{"id":"u3","score":1.5}]}'
# HTTP 400
# {"error":"field \"score\": number must be an integer","index":1,"id":"u3"}
```

与已有记录完全相同的写入是幂等的（返回 200，`inserted` 为 0）。

### 替换已导入的记录

请求体可选字段 `replace`（布尔值，默认 `false`，缺省时行为与上述完全一致）。设为
`true` 时，允许用新内容覆盖同主键的已有记录：同主键不同内容不再整批拒绝，而是被替换；
响应中 `replaced` 为被替换的记录条数，`inserted` 仍为新增条数，其余字段含义不变。

```sh
curl -s -X POST http://127.0.0.1:8080/collections/users/records \
  -H 'Content-Type: application/json' \
  -d '{"replace":true,"records":[{"id":"u1","name":"Ada","age":37}]}'
# {"collection":"users","accepted":1,"inserted":0,"replaced":1}
```

替换只影响当前数据：已保存版本永不改变，之后保存的版本与旧版本做差异比较时，被替换的
记录出现在 `changed` 中（`before` 为旧记录、`after` 为新记录）。字段顺序按新记录的写入
顺序保留，但比较内容时字段顺序不影响判定（顺序不同、内容相同仍为幂等，不计入
`replaced`）。同一批内主键重复、记录非法、空批次等校验与 `replace=false` 完全一致，
任一条不合法时整批不生效，不会发生部分替换。带替换的批次同样先追加到 WAL 并 fsync
才确认成功，重启后已确认的替换与替换后的当前数据一致。

### 删除记录（撤回误导入）

请求体可选字段 `delete`（字符串主键数组，缺省时按写入处理，行为与上文完全一致）。
`delete` 非空时该请求为删除请求：按数组中给出的主键逐条删除当前数据中的记录。

```sh
curl -s -X POST http://127.0.0.1:8080/collections/users/records \
  -H 'Content-Type: application/json' \
  -d '{"delete":["u1","u3","ghost"]}'
# {"collection":"users","accepted":3,"deleted":2,"missing":1}
```

返回字段：`deleted` 为实际被删除的条数，`missing` 为给定主键中集合里不存在的条数——
主键不存在保持幂等，不计数到 `deleted`、也不报错。删除整批同样全有或全无，以下情况
整批拒绝（HTTP 400），并返回 `error`、`index` 与可识别时的 `id`：

- `delete` 为空数组，或元素不是字符串、为空字符串；
- `delete` 中出现重复主键；
- `delete` 与 `records` 同时出现。

删除只影响当前数据：已保存版本永不改变，之后保存的版本与旧版本做差异比较时，
被删除的记录出现在 `added` 中（`before` 为旧记录，语义不变）。删除后再写入同主键
同内容记录算新增插入（`inserted` 计 1），同主键不同内容仍按既有 `replace` 规则处理。
删除同样先追加到 `wal.log` 并 fsync 才确认成功：重启后已确认删除与删除后的当前数据
一致，未确认的删除不产生任何效果。合法删除不会自动创建不存在的集合（全部计入
`missing`）；既有空批次与非法记录校验、集合首次写入自动创建等行为不变。

### 保存版本

```sh
curl -s -X POST http://127.0.0.1:8080/versions
# HTTP 201
# {"id":1,"saved_at":1790929220820}
```

`id` 可重复用于后续查询，`saved_at` 为保存时间（Unix 纪元毫秒）。快照一经保存即只读，
之后的写入、替换、删除或清空都不会影响它。并发写入、删除与保存时，每个快照都完整对应
某次保存动作之前已确认提交的全部批次，不会出现半批记录。

### 查询历史版本

```sh
curl -s http://127.0.0.1:8080/versions/1/collections/users/records
# {"version":1,"collection":"users","records":[{"id":"u1", ...}, {"id":"u2", ...}]}

curl -s http://127.0.0.1:8080/versions
# {"versions":[{"id":1,"saved_at":1790929220820}]}

curl -s http://127.0.0.1:8080/versions/1
# {"id":1,"saved_at":1790929220820,"collections":["users"]}
```

版本不存在或该版本中没有指定集合时返回 HTTP 404 与明确的错误信息，且不改变任何数据：

```text
{"error":"version 999 not found"}
{"error":"collection \"orders\" not found in version 1"}
```

### 比较两个版本的差异

```sh
curl -s http://127.0.0.1:8080/versions/1/collections/users/diff/2
```

路径参数依次为：起始版本 `from`、集合名、目标版本 `to`。版本号随保存动作递增，
`from` 不得晚于 `to`；`from` 与 `to` 相同版本时三个列表都为空。响应示例：

```json
{
  "from": 1,
  "to": 2,
  "collection": "users",
  "added":   [{"id": "u9", "name": "Gone"}],
  "dropped": [{"id": "u3", "name": "New", "age": 20}],
  "changed": [{"id": "u1",
               "before": {"id": "u1", "name": "Ada", "age": 36},
               "after":  {"id": "u1", "name": "Ada", "age": 37}}]
}
```

结果字段含义：

- `added`：仅存在于起始版本的记录（即在目标版本中被删除的记录）。
- `dropped`：仅存在于目标版本的记录（即两个版本之间新写入的记录）。
- `changed`：两个版本中都存在但内容不同的记录，`before`/`after` 分别为
  修改前（起始版本）与修改后（目标版本）的完整记录。

三个列表内部均按主键 `id` 的字节序升序排列。比较记录内容时字段顺序不影响判定，
字段名与字段值相同即视为同一条记录。差异查询只读取已保存的快照：不会生成新版本、
不改写任何记录，也不影响并发写入。

错误处理与查询接口一致，且均不改变任何数据：

- 起始或目标版本不存在：HTTP 404，`{"error":"version 999 not found"}`；
- 集合在其中任一版本中不存在：HTTP 404，
  `{"error":"collection \"orders\" not found in version 1"}`；
- 起始版本晚于目标版本：HTTP 400，
  `{"error":"start version 2 is later than target version 1"}`。

## 持久化与崩溃恢复

- 每批确认成功的写入或删除先追加到 `wal.log` 并 fsync，再更新内存；进程崩溃时未落盘的
  请求不会产生任何记录，末尾的残缺行会在下次启动时自动截断。
- 每次保存版本会把全部版本与当前数据以原子重命名方式写入 `snapshot.json`，随后压缩
  WAL。进程重启后已保存版本及其数据仍可查询。

```sh
# 核对方式：写入并保存版本后 kill -9 进程，再用同一 VDE_DATA 启动，
# 旧版本数据、版本列表以及保存之后已确认的写入都仍可查询。
```

## 测试

```sh
cargo test
```

覆盖：整批原子性与各类校验拒绝、快照只读与字段顺序、版本/集合不存在报错、
版本差异（added/dropped/changed、排序、顺序与存在性错误）、
同主键替换（replaced/inserted 计数、原子性、字段顺序、历史版本不变与重启恢复）、
按主键删除（deleted/missing 计数与幂等、空数组/非字符串/重复主键/与 records 同现的
整批拒绝、删后重写为新增、历史版本不变、diff 中出现在 added、重启恢复与未确认丢失）、
并发写入、删除与快照的无半批一致性、重启恢复与残缺 WAL 截断。
