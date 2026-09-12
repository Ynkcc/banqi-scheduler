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
| `cmd/scheduler/main.go` | 入口，配置全走 `SCHEDULER_*` 环境变量（`-h` 列出）；示例配置见 `config.example.env`（含 R2 凭据与 GSPRT 参数说明） |
| `internal/store` | SQLite 元数据（networks/matches/episodes/workers/settings，best 指针事务切换） |
| `internal/r2` | 预签名 PUT/GET，键布局 `episodes/<sha>/*.jsonl.gz`、`networks/<sha>.bin` |
| `internal/sprt` | 五项 GSPRT（正态近似 LLR，elo0/elo1/alpha/beta 可配，含单测） |
| `internal/scheduler` | gRPC 服务实现 + 任务表（内存 task_id 注册校验）；`control.go` 为运行时控制状态（落库 settings） |
| `internal/api` | WebUI 的 JSON API + 控制端点 + embed 前端产物（同进程 http.Server） |
| `webui/` | React 18 + Vite + TS + AntD 前端源码，构建产物输出到 `internal/api/dist` |
| `pb/` | protoc 生成代码（不提交） |
| `proto/scheduler.proto` | 契约源文件（已随拆分迁入；主仓库 `build.rs` 仍编译自己的 `proto/scheduler.proto` 副本，proto 变更需双侧同步） |

## 3. gRPC 契约（scheduler.proto，9 RPC）

- `GetTask`：worker 按机器规格拉任务（优先 gatekeeper rating，其次 best 网络 selfplay），按 worker 线程数缩放下发局数（`SCHEDULER_THREADS_BASELINE`）；selfplay 与 rating 均在 `SelfPlayParams.extra_config` 下发课程参数 `{"initial_revealed_pieces":N}`（<=0 不下发，worker 用变体默认值）——课程阶段切换可改 `SCHEDULER_INITIAL_REVEALED` 重启调度器，也可经 WebUI 在线切换（见 §5），worker 无需重编译；
- `ReportEpisode`：只收元数据，签发 R2 预签名 PUT，数据直传 R2（校验 task↔worker 归属）；
- `GetNetwork`：sha 或 best → 预签名 GET；
- `RegisterNetwork`：trainer 登记新网络 → 自动创建 gatekeeper 对打；首个网络直接晋级；
- `ReportMatchResult`：五项成对计数累计 → GSPRT 判停 → 晋级/拒绝 best 指针；
- `Heartbeat`：worker 状态（client_version/memory_mb/running_task_id）+ best sha 下发；
- `SignNetworkUpload`：trainer 请求网络直传预签名 PUT；
- `ListEpisodes`：trainer 游标分页拉 episode 预签名 GET 列表；
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
| `POST /api/control` | `{"paused":bool}` 暂停 selfplay（rating 继续：`GetTask` 只回 rating、`Heartbeat` 回 `pause_self_play`）；`{"initialRevealed":int}` 在线切换课程阶段，经 `extra_config` 下发 |

已知限制：`scheduler.Server.tasks` 只做 task↔worker 归属校验、从不清理，长期运行会缓慢增长（`/api/tasks` 一并展示该内存表）。

## 6. 变更记录

- 2026-09-11：从主仓库 `docs/ARCHITECTURE.md` §6.4 拆出，作为未来独立仓库的架构文档。
- 2026-09-11：经 `git subtree split` 自主仓库 `server/` 拆出为独立仓库（保留完整提交历史）；`proto/scheduler.proto` 迁入本仓库。
- 2026-09-11：新增 `SCHEDULER_INITIAL_REVEALED`（课程学习初始翻子数），经 `SelfPlayParams.extra_config` 下发（selfplay + rating 均生效），proto 无变更。
- 2026-09-12：新增 WebUI：`internal/api`（同进程 HTTP JSON API + 控制端点 + embed 前端产物，`SCHEDULER_HTTP_ADDR`/`SCHEDULER_WORKER_ONLINE_SECONDS`）、`webui/`（React 18 + Vite + TS + AntD SPA）；`store` 新增 `settings` 表与只读列表/统计查询；`scheduler` 新增运行时 `Control`（暂停 selfplay、课程阶段切换，落库并优先于环境变量，`New` 改为返回 error），proto 无变更。
