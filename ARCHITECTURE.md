# ARCHITECTURE — banqi-scheduler（Go 中心调度器）

> **仓库状态**：已从主仓库 `rust_4x8`（`server/` 子树，含完整提交历史）拆分为独立私有仓库 `Ynkcc/banqi-scheduler`。
> 主仓库侧对应文档为 `docs/ARCHITECTURE.md` §6.4（拆分后的引用摘要）。

## 1. 定位

分布式自对弈训练的中心调度器：任务分发（selfplay/rating）、网络登记与晋级、episode 元数据登记 + R2 预签名直传、五项 GSPRT 判停。

调研结论（主仓库 `docs/distributed_training_reference_survey.md`）落地：lczero 拉取式调度 + KataGo URL 下发/预签名直传 + fishtest/pentanomial 五项 GSPRT 判停。
技术栈：Go + grpc-go + SQLite（modernc 纯 Go 驱动，WAL）+ aws-sdk-go-v2 S3 预签名（R2 兼容，凭据走标准 `AWS_*` 环境变量）。

## 2. 结构

| 条目 | 说明 |
|---|---|
| `go.mod` | module `banqi/server` |
| `cmd/scheduler/main.go` | 入口：`run()` 编排生命周期（`signal.NotifyContext` → `GracefulStop` + WebUI `Shutdown`），配置全走 `SCHEDULER_*` 环境变量（`-h` 列出）；示例配置见 `config.example.env`（含 R2 凭据与 GSPRT 参数说明）。另读标准 `AWS_*` 凭据/endpoint，`AWS_S3_PATH_STYLE=1` 时改用 path-style 寻址（本地 RustFS / MinIO 必需，R2 默认不设） |
| `internal/store` | SQLite 元数据，全部方法以 `context` 为首参；文件按表拆分：`store.go`（连接/迁移/统计/设置）、`networks.go`、`matches.go`、`episodes.go`、`workers.go`；迁移用 `pragma_table_info` 探测列存在性，best 指针事务切换 |
| `internal/r2` | 预签名 PUT/GET，键布局 `episodes/<sha>/*.epb.gz`（EpisodeBatch 二进制记录）、`networks/<sha>.<format>`（扩展名 = 权重格式 onnx/pt/nnue，worker 据此分派加载器） |
| `internal/sprt` | 五项 GSPRT（正态近似 LLR，elo0/elo1/alpha/beta 可配，含单测） |
| `internal/scheduler` | gRPC 服务实现 + 内存任务表（task↔worker 归属校验、rating 在飞判定、超期回收）；文件按职责拆分：`server.go`（运行状态与全局 RPC）、`tasks.go`（GetTask/ratingTask，下发 `network_key`/`opponent_key`）、`networks.go`（登记/晋级/gatekeeper 判停 + 权重格式白名单校验）、`episodes.go`（episode 登记与 trainer 数据面）、`control.go`（运行时控制，落库 settings） |
| `internal/api` | WebUI 的 JSON API + 控制端点 + embed 前端产物（`HTTPServer(addr)` 交由调用方启停，同进程 http.Server） |
| `webui/` | React 18 + Vite + TS + AntD 前端源码，构建产物输出到 `internal/api/dist` |
| `pb/` | protoc 生成代码（不提交） |
| `proto/scheduler.proto` | 契约源文件（已随拆分迁入；主仓库 `build.rs` 仍编译自己的 `proto/scheduler.proto` 副本，proto 变更需双侧同步） |

## 3. gRPC 契约（scheduler.proto，9 RPC）

- `GetTask`：worker 按机器规格拉任务（优先 gatekeeper rating，其次 best 网络 selfplay），按 worker 线程数缩放下发局数（`SCHEDULER_THREADS_BASELINE`）；恒下发网络对象键 `network_key`（rating 任务另含 `opponent_key`），下载 URL 仅在 worker 需要拉取时签发；selfplay 与 rating 均在 `SelfPlayParams.extra_config` 下发课程参数 `{"initial_revealed_pieces":N}`（<=0 不下发，worker 用变体默认值）——课程阶段切换可改 `SCHEDULER_INITIAL_REVEALED` 重启调度器，也可经 WebUI 在线切换（见 §5），worker 无需重编译；selfplay 另经 `SelfPlayParams.data_kind` 下发**数据类别**（`SCHEDULER_DATA_KIND` / WebUI，见 `DataKind`），worker 据此产出对应类别的记录；
- `ReportEpisode`：只收元数据（含数据类别 `kind`，落库 `episodes.kind`），签发 R2 预签名 PUT（对象键 `episodes/<sha>/<id>.epb.gz`），数据直传 R2（校验 task↔worker 归属）；
- `GetNetwork`：sha 或 best → 对象键 + 预签名 GET；
- `RegisterNetwork`：trainer 登记新网络（含权重格式 `format`，落库 `networks.format`）→ 自动创建 gatekeeper 对打；首个网络直接晋级；格式不在白名单（onnx/pt/nnue）时拒绝登记；
- `ReportMatchResult`：五项成对计数累计 → GSPRT 判停 → 晋级/拒绝 best 指针；
- `Heartbeat`：worker 状态（client_version/memory_mb/running_task_id）+ best sha 下发；
- `SignNetworkUpload`：trainer 请求网络直传预签名 PUT（须声明权重格式 `format`，对象键 `networks/<sha>.<format>`）；
- `ListEpisodes`：trainer 游标分页拉 episode 预签名 GET 列表（游标为上次返回的对象键，服务端据此解析 `episodes.id` 并按登记顺序推进，不依赖对象键字典序）；可带 `kind` 只取某一数据类别（缺省不过滤），消费方据此避免下载无法消费的对象；
- `GetInfo`：返回 `variant`（变体类型由服务端下发，`SCHEDULER_VARIANT` 配置）。

**安全约定**：R2 凭据只在调度器持有，worker/trainer 零存储配置，全部经预签名 URL 上下行。

## 4. proto 生成

```
protoc --proto_path=proto --go_out=. --go_opt=module=banqi/server \
  --go-grpc_out=. --go-grpc_opt=module=banqi/server proto/scheduler.proto
```

## 5. WebUI（同进程 HTTP，默认仅本机）

`cmd/scheduler` 在同一进程内起 `http.Server`（`SCHEDULER_HTTP_ADDR`，默认 `127.0.0.1:8080`），复用同一 `store` 与 `scheduler.Server` 状态，不经过 gRPC，无鉴权。WebUI 监听失败只记日志，不影响 gRPC 调度。

前端产物 embed 进二进制，构建顺序与「先 protoc 生成 `pb/`」一致：

```
cd webui && npm install && npm run build   # 输出到 internal/api/dist
go build ./cmd/scheduler
```

只读接口：

| 路由 | 说明 |
|---|---|
| `GET /api/status` | 变体、课程阶段、暂停状态、SPRT 参数、best、计数 |
| `GET /api/networks` | 网络列表（best 优先） |
| `GET /api/matches` | 对战列表，附 LLR / 得分率 / 判停结果 |
| `GET /api/workers` | Worker 列表与在线判定（`SCHEDULER_WORKER_ONLINE_SECONDS`，默认 60s） |
| `GET /api/episodes?before=&limit=` | episode 元数据按 id 倒序分页 |
| `GET /api/tasks` | 进程内进行中任务快照 |

控制接口（写入 `settings` 表并跨重启保留；环境变量仅在库中无记录时作为初始值）：

| 路由 | 说明 |
|---|---|
| `POST /api/networks/{sha}/promote` | 手动晋级 best |
| `POST /api/control` | `{"paused":bool}` 暂停 selfplay（rating 继续：`GetTask` 只回 rating、`Heartbeat` 回 `pause_self_play`）；`{"initialRevealed":int}` 在线切换课程阶段，经 `extra_config` 下发；`{"dataKind":"resnet"\|"nnue"}` 在线切换自对弈产出的数据类别，经 `SelfPlayParams.data_kind` 下发 |

内存任务表保留策略（见 `internal/scheduler/server.go`）：tasks 仅用于 task↔worker 归属校验与 rating 在飞判定，进度以 DB 为准；已上报结束（`Done`）的记录保留 `doneTaskRetention`(30m)、未上报记录最多保留 `taskMaxRetention`(24h)，由 `pruneTasks` 在 `GetTask` 路径上按 `pruneInterval`(1m) 节流回收；`/api/tasks` 只展示未结束任务。

## 6. 变更记录

- 2026-09-11：从主仓库 `docs/ARCHITECTURE.md` §6.4 拆出，作为未来独立仓库的架构文档。
- 2026-09-11：经 `git subtree split` 自主仓库 `server/` 拆出为独立仓库（保留完整提交历史）；`proto/scheduler.proto` 迁入本仓库。
- 2026-09-11：新增 `SCHEDULER_INITIAL_REVEALED`（课程学习初始翻子数），经 `SelfPlayParams.extra_config` 下发（selfplay + rating 均生效），proto 无变更。
- 2026-09-12：新增 WebUI：`internal/api`（同进程 HTTP JSON API + 控制端点 + embed 前端产物，`SCHEDULER_HTTP_ADDR`/`SCHEDULER_WORKER_ONLINE_SECONDS`）、`webui/`（React 18 + Vite + TS + AntD SPA）；`store` 新增 `settings` 表与只读列表/统计查询；`scheduler` 新增运行时 `Control`（暂停 selfplay、课程阶段切换，落库并优先于环境变量，`New` 改为返回 error），proto 无变更。
- 2026-09-14：修正 `ListEpisodes` 游标语义——原按 `object_key` 字典序推进，而对象键含随机段（`episodes/<sha>/<random>.jsonl.gz`），字典序与登记顺序无关，会导致已登记但键更小的 episode 永久不出现在列表里（trainer 静默丢数据）。改为以 `afterKey` 反查 `episodes.id` 后按 id 递增返回，并补 `idx_episodes_object_key`；proto 与客户端无需变更。
- 2026-09-14：`internal/r2` 新增 `AWS_S3_PATH_STYLE`（默认关闭）以支持 RustFS / MinIO 等无法 virtual-host 寻址的本地 S3 实现；启动时打印 `path_style` 便于排查。R2 侧行为不变。
- 2026-09-15：运行时健壮性改造：①`cmd/scheduler/main.go` 改为 `run()` + `signal.NotifyContext`，收到 SIGINT/SIGTERM 后 `grpcServer.GracefulStop()` 并 `Shutdown` WebUI，启动失败路径不再被 `log.Fatalf` 跳过 defer；②内存任务表新增超期回收（`pruneTasks`，`Done` 保留 30m / 未上报最多 24h，`pruneInterval` 节流），`/api/tasks` 不再展示已结束任务；③`internal/store` 全部方法改为 ctx 首参（`QueryContext`/`ExecContext`/`BeginTx`），调用方传请求 ctx；④建表迁移改用 `pragma_table_info` 探测列存在性（删除依赖驱动错误文案的 `duplicate column name` 匹配与 `q[:40]` 切片），`RowsAffected` 错误不再被忽略；⑤`newID` 由 panic 改为返回错误；⑥gRPC 层错误补上下文（`fmt.Errorf("...: %w")`）；⑦`internal/api` 的 `ListenAndServe` 改为 `HTTPServer(addr)`，生命周期交由 `cmd/scheduler` 编排。新增 `internal/store/store_test.go`、`internal/scheduler/server_test.go`。
- 2026-09-15：文件拆分（降低单文件认知负荷）：`internal/store/store.go`(455 行) 拆为 `store.go` / `networks.go` / `matches.go` / `episodes.go` / `workers.go`（并抽出 `matchColumns` 统一列序）；`internal/scheduler/server.go`(495→544 行) 拆为 `server.go` / `tasks.go` / `networks.go` / `episodes.go`（新增 `lookupTask` / `markTaskDone` / `registerTask` / `judgeMatch` 辅助），单文件均 < 250 行。新增 `internal/api/handlers_test.go`（status/tasks/promote/control 与错误码）。`internal/sprt/sprt.go` 补 gofmt。
- 2026-09-16：**对象键自描述与训练数据记录 schema 化**（一侧改动，两侧契约同步——proto 变更已同步到 banqi-collector / banqi-training 副本）：
  - **episode 键**：`episodes/<sha>/<id>.jsonl.gz` → `episodes/<sha>/<id>.epb.gz`（载荷为 `EpisodeBatch` 二进制，schema 定义在 proto）。
  - **权重键**：`networks/<sha>.bin` → `networks/<sha>.<format>`；`networks` 表新增 `format` 列（默认 `onnx`，迁移按 `pragma_table_info` 探测补列），权重格式由上传方在 `SignNetworkUpload` 与 `RegisterNetwork` 两处声明并由调度器按白名单（onnx/pt/nnue）校验，任一为空或非法即拒绝，避免生成无法下载的键。
  - **下发给 worker 的对象键**：`TaskResponse.network_key` / `opponent_key` 恒下发（本地缓存命名与权重格式判定的依据），`NetworkInfo.key` 供心跳预取使用；下载 URL 仍在需要拉取时才签发。旧缓存与旧对象作废（用户确认不做兼容）。
  - **新增训练数据记录消息**：`EpisodeBatch` / `EpisodeRecord` / `NnueEpisodeRecord` / `NnueFeatures` / `NnueMeta`（字段号 + `schema_version`）。调度器只登记元数据、不解析对象内容，本组消息由采集端与训练端共享。
- 2026-09-16：**数据类别（ResNet / NNUE）贯通为可在线切换的服务端配置**（本轮不含采集端产出 NNUE 的能力）：
  - **proto**：新增 `enum DataKind`（`DATA_RESNET` / `DATA_NNUE`），落到 `SelfPlayParams.data_kind`（任务要产哪类）、`EpisodeBatch.kind`（对象自描述）、`EpisodeMeta.kind`（落库）、`ListEpisodesRequest.kind`（消费端按类别过滤，缺省不过滤）。
  - **类别来源**：`SCHEDULER_DATA_KIND`（默认 `resnet`）作为库中无记录时的初值，运行时以 `Control.dataKind` 为准，可经 WebUI `POST /api/control` 在线切换并落库 `settings.data_kind`。
  - **落库与过滤**：`episodes` 表新增 `kind` 列（迁移按 `pragma_table_info` 补列，缺省 0 = ResNet），`ListEpisodeKeys` 支持按类别过滤（游标子查询独立于过滤条件，跨类别切换游标仍能正确推进）。
  - **`EpisodeRecord.nnue` 字段废弃**（`reserved 21`）：MCTS 自对弈不再顺带收集 NNUE 稀疏特征，两类数据彻底分家——NNUE 特征只由 `NnueEpisodeRecord` 承载；`NnueEpisodeRecord` 改为内嵌 `NnueFeatures`，稀疏特征的布局定义只此一处。
  - **语义边界**：调度器只负责下发类别，不校验 worker 是否具备该采集能力；当前 `banqi-collector` 仅支持 `DATA_RESNET`，收到 `DATA_NNUE` 任务会明确报错退出（绝不静默产出别类数据）。
