# ARCHITECTURE — banqi-scheduler（Go 中心调度器）

> **仓库规划**：本目录（`server/` + 根 `proto/scheduler.proto` + `deploy/`）计划拆分为独立仓库 `banqi-scheduler`。
> 拆分前随主仓库维护，本文档即拆出后的架构文档；本文与主仓库 `docs/ARCHITECTURE.md` §6.4 对应，以本文为准。

## 1. 定位

分布式自对弈训练的中心调度器：任务分发（selfplay/rating）、网络登记与晋级、episode 元数据登记 + R2 预签名直传、五项 GSPRT 判停。

调研结论（`../docs/distributed_training_reference_survey.md`）落地：lczero 拉取式调度 + KataGo URL 下发/预签名直传 + fishtest/pentanomial 五项 GSPRT 判停。
技术栈：Go + grpc-go + SQLite（modernc 纯 Go 驱动，WAL）+ aws-sdk-go-v2 S3 预签名（R2 兼容，凭据走标准 `AWS_*` 环境变量）。

## 2. 结构

| 条目 | 说明 |
|---|---|
| `go.mod` | module `banqi/server` |
| `cmd/scheduler/main.go` | 入口，配置全走 `SCHEDULER_*` 环境变量（`-h` 列出）；示例配置见 `config.example.env`（含 R2 凭据与 GSPRT 参数说明） |
| `internal/store` | SQLite 元数据（networks/matches/episodes/workers，best 指针事务切换） |
| `internal/r2` | 预签名 PUT/GET，键布局 `episodes/<sha>/*.jsonl.gz`、`networks/<sha>.bin` |
| `internal/sprt` | 五项 GSPRT（正态近似 LLR，elo0/elo1/alpha/beta 可配，含单测） |
| `internal/scheduler` | gRPC 服务实现 + 任务表（内存 task_id 注册校验） |
| `pb/` | protoc 生成代码（不提交） |
| `../proto/scheduler.proto` | 契约源文件（**拆分时迁入本仓库**；主仓库 Rust build.rs 目前也编译它，拆分后主仓库改为引用本仓库的 proto 副本或直接拷贝） |

## 3. gRPC 契约（scheduler.proto，9 RPC）

- `GetTask`：worker 按机器规格拉任务（优先 gatekeeper rating，其次 best 网络 selfplay），按 worker 线程数缩放下发局数（`SCHEDULER_THREADS_BASELINE`）；
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
protoc --proto_path=proto --go_out=server --go_opt=module=banqi/server \
  --go-grpc_out=server --go-grpc_opt=module=banqi/server scheduler.proto
```

（拆分后 `--go_out=.` 并去掉 module 前缀。）

## 5. 变更记录

- 2026-09-11：从主仓库 `docs/ARCHITECTURE.md` §6.4 拆出，作为未来独立仓库的架构文档。
