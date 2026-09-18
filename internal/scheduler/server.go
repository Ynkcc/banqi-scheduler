// Package scheduler 实现 gRPC 调度器：任务分发、网络登记与晋级、episode 元数据登记。
//
// 文件划分：
//   - server.go    运行状态与生命周期（Server / Runtime / 内存任务表回收 / 全局状态 RPC）
//   - tasks.go     任务下发（GetTask / ratingTask）
//   - networks.go  网络登记与 gatekeeper 判停（GetNetwork / RegisterNetwork / ReportMatchResult）
//   - episodes.go  episode 登记与 trainer 数据面（ReportEpisode / SignNetworkUpload / ListEpisodes）
package scheduler

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"banqi/server/internal/r2"
	"banqi/server/internal/store"
	pb "banqi/server/pb"
)

type Config struct {
	Variant          string // 服务端下发的变体（4x8 / 4x4 / 4x2）
	GamesPerTask     int    // selfplay 每次下发的局数
	GatekeeperGames  int    // gatekeeper 对打目标局数（成对）
	SprtElo0         float64
	SprtElo1         float64
	SprtAlpha        float64
	SprtBeta         float64
	MinClientVersion string
	ThreadsBaseline  int // 资源分配基准线程数：games = GamesPerTask × threads/baseline（<=0 则不缩放）
	InitialRevealed  int // 课程学习初始值：仅当库中无记录时生效（运行时以 Control 为准）
	// DataKind 自对弈任务产哪类数据（ResNet/MCTS 或 NNUE）；同样仅在库中无记录时生效。
	DataKind pb.DataKind
	// ReanalysisIntervalTasks 每 N 个 selfplay 任务最多下发 1 个局面重搜任务（0 = 关闭重搜）。
	// 重搜与自对弈争抢同一份算力，故按间隔节流（见 reanalysis.go）。
	ReanalysisIntervalTasks int
	// ReanalysisMaxQueue 待下发重搜任务的队列上限（按载荷条数，<=0 视为不限）。
	ReanalysisMaxQueue int

	// ---- 绝对强度评估（TASK_EVAL，见 eval.go）----
	// EvalEnabled 是否启用评估：关闭时不下发任何 eval 任务（老 collector 未升级时先别开）。
	EvalEnabled bool
	// EvalOpponents 评估对手标识列表（random / rule:capture_first / rule:reveal_first）。
	EvalOpponents []string
	// EvalGames 每次评估的局数（一次任务即含全部局数，不做累加）。
	EvalGames int
	// EvalMctsSims 评估搜索深度：0 = 纯策略 argmax（门禁主口径），>0 = MCTS 模拟数。
	EvalMctsSims int
	// EvalEveryNPromotions 节流：每 N 次 best 晋级评测一次（<=0 视为 1）。
	EvalEveryNPromotions int
	// EvalNoProgressN 连续 N 次评测提升不足即置位 should_stop（<=0 关闭判停，只观测）。
	EvalNoProgressN int
	// EvalNoProgressEps 判定「有提升」的最小胜率增量（0.02 = 2pt，对齐 n=3000 时 2σ≈1.8pt）。
	EvalNoProgressEps float64
}

// extraConfig 生成 SelfPlayParams.extra_config（JSON 透传）；无课程参数时为空串。
func (s *Server) extraConfig() string {
	n := s.ctl.InitialRevealed()
	if n <= 0 {
		return ""
	}
	return fmt.Sprintf(`{"initial_revealed_pieces":%d}`, n)
}

// gamesFor 按 worker 线程数缩放本批局数（资源分配；threads<=0 视为 1）。
func (s *Server) gamesFor(threads int32) int32 {
	if s.cfg.ThreadsBaseline <= 0 {
		return int32(s.cfg.GamesPerTask)
	}
	t := int(threads)
	if t < 1 {
		t = 1
	}
	g := s.cfg.GamesPerTask * t / s.cfg.ThreadsBaseline
	if g < 1 {
		g = 1
	}
	return int32(g)
}

type runningTask struct {
	Kind       pb.TaskKind
	MatchID    int64
	NetworkSha string
	// OpponentSha rating 任务的对手网络 sha；eval 任务恒为空（对手不是网络）。
	OpponentSha string
	// OpponentSpec eval 任务的对手标识（rule:capture_first 等）；其余任务为空。
	OpponentSpec string
	Games        int
	WorkerID     string
	CreatedAt    time.Time
	// Done 标记任务已上报结束（rating / eval 用）：Done 后即释放对应的在飞名额。
	Done bool
}

// 内存任务表（tasks）保留策略。tasks 仅用于 task↔worker 归属校验与在飞判定，
// 进度以 DB 为准，因此超期记录可安全回收，避免长期运行内存无限增长。
const (
	// ratingTaskStaleAfter：rating 任务超过该时长未上报即视为失效（worker 掉线）。
	ratingTaskStaleAfter = 10 * time.Minute
	// doneTaskRetention：已上报结束的任务记录保留时长（保留一小段，便于重复上报得到明确错误）。
	doneTaskRetention = 30 * time.Minute
	// taskMaxRetention：未上报任务记录的最大保留时长（正常批次远短于此）。
	taskMaxRetention = 24 * time.Hour
	// pruneInterval：回收扫描的最小间隔（节流，避免每次请求都全表扫描）。
	pruneInterval = time.Minute
)

// ratingInFlight 判断某 match 是否已有 rating 任务在飞。
// 同一 match 被多个 worker 并发领取会导致重复上报（后到者必被
// match_not_running 拒绝），这里保证同刻只有一个在飞任务；
// 超过 ratingTaskStaleAfter 未上报的任务（worker 崩了）视为失效。
func (s *Server) ratingInFlight(matchID int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, t := range s.tasks {
		if t.Kind == pb.TaskKind_TASK_RATING && t.MatchID == matchID && !t.Done &&
			time.Since(t.CreatedAt) < ratingTaskStaleAfter {
			return true
		}
	}
	return false
}

// pruneTasks 回收内存任务表中的超期记录；now 由调用方传入以便测试。
// 内部按 pruneInterval 节流，可在请求路径上安全高频调用。
func (s *Server) pruneTasks(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.lastPrune.IsZero() && now.Sub(s.lastPrune) < pruneInterval {
		return
	}
	s.lastPrune = now
	removed := 0
	for id, t := range s.tasks {
		age := now.Sub(t.CreatedAt)
		if (t.Done && age >= doneTaskRetention) || age >= taskMaxRetention {
			delete(s.tasks, id)
			removed++
		}
	}
	if removed > 0 {
		log.Printf("[task] 回收超期任务记录 %d 条，内存表剩余 %d 条", removed, len(s.tasks))
	}
}

// RunningTask 是进行中任务的只读快照（WebUI 展示用）。
type RunningTask struct {
	TaskID       string
	WorkerID     string
	Kind         pb.TaskKind
	MatchID      int64
	NetworkSha   string
	OpponentSha  string
	OpponentSpec string
	Games        int
	CreatedAt    time.Time
}

// Runtime 是调度器运行时配置快照（含 WebUI 可写项）。
type Runtime struct {
	Variant          string
	MinClientVersion string
	SprtElo0         float64
	SprtElo1         float64
	SprtAlpha        float64
	SprtBeta         float64
	Paused           bool
	InitialRevealed  int
	DataKind         pb.DataKind
	// 绝对强度评估（只读快照；判停信号见 ShouldStop/StopReason）
	EvalEnabled          bool
	EvalOpponents        []string
	EvalGames            int
	EvalMctsSims         int
	EvalNoProgressN      int
	EvalNoProgressEps    float64
	EvalEveryNPromotions int
}

// Presigner 接口（r2.Presigner 满足；测试可注入 fake）。预签名 PUT/GET 是副作用
// 通道（HTTP 出向），用接口抽象便于在不动 AWS SDK 的前提下做幂等/契约测试。
type Presigner interface {
	PresignPut(ctx context.Context, key string, length int64) (string, error)
	PresignGet(ctx context.Context, key string) (string, error)
}

type Server struct {
	pb.UnimplementedSchedulerServiceServer
	cfg   Config
	ctl   *Control
	store *store.Store
	r2    Presigner

	mu        sync.Mutex
	tasks     map[string]*runningTask
	lastPrune time.Time

	// 待下发的重搜任务（trainer 提交，FIFO）与「自上次下发重搜以来的 selfplay 任务数」（节流）。
	// 二者与 tasks 共用 s.mu 保护。
	reanalysis              []*pendingReanalysis
	reanalysisSinceSelfplay int

	// 待下发的评估任务（best 晋级时入队，见 eval.go）与「已触发的晋级次数」（节流计数）。
	// 同样与 tasks 共用 s.mu 保护。计数留内存：重启后节流相位重置可接受（评测本身幂等）。
	evals          []*pendingEval
	evalPromotions int
}

func New(ctx context.Context, cfg Config, st *store.Store, presigner *r2.Presigner) (*Server, error) {
	ctl, err := loadControl(ctx, st, cfg.InitialRevealed, cfg.DataKind)
	if err != nil {
		return nil, fmt.Errorf("load control: %w", err)
	}
	s := &Server{cfg: cfg, ctl: ctl, store: st, r2: presigner, tasks: make(map[string]*runningTask)}
	// 启动恢复：把上次进程退出时尚未收尾的 eval 待办从 DB 拉回内存。
	// 否则重启会丢失「同一 best 的部分评测」，且与 ensureBestEvaluated 补齐逻辑
	// 重叠后可能造成 (best, spec) 同时存在「in-flight」与「queued」的歧义。
	if err := s.loadEvalQueue(ctx); err != nil {
		return nil, fmt.Errorf("load eval queue: %w", err)
	}
	// 启动补齐：当前 best 若有未评测的对手（新增评测配置 / 上次被中断）立即入队。
	s.ensureBestEvaluated(ctx)
	return s, nil
}

func (s *Server) Control() *Control { return s.ctl }

func (s *Server) Runtime() Runtime {
	return Runtime{
		Variant:          s.cfg.Variant,
		MinClientVersion: s.cfg.MinClientVersion,
		SprtElo0:         s.cfg.SprtElo0,
		SprtElo1:         s.cfg.SprtElo1,
		SprtAlpha:        s.cfg.SprtAlpha,
		SprtBeta:         s.cfg.SprtBeta,
		Paused:           s.ctl.Paused(),
		InitialRevealed:  s.ctl.InitialRevealed(),
		DataKind:         s.ctl.DataKind(),

		EvalEnabled:          s.cfg.EvalEnabled,
		EvalOpponents:        s.cfg.EvalOpponents,
		EvalGames:            s.cfg.EvalGames,
		EvalMctsSims:         s.cfg.EvalMctsSims,
		EvalNoProgressN:      s.cfg.EvalNoProgressN,
		EvalNoProgressEps:    s.cfg.EvalNoProgressEps,
		EvalEveryNPromotions: s.cfg.EvalEveryNPromotions,
	}
}

func (s *Server) RunningTasks() []RunningTask {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]RunningTask, 0, len(s.tasks))
	for id, t := range s.tasks {
		if t.Done {
			continue // 已上报结束的任务不再展示为「进行中」
		}
		out = append(out, RunningTask{
			TaskID: id, WorkerID: t.WorkerID, Kind: t.Kind, MatchID: t.MatchID,
			NetworkSha: t.NetworkSha, OpponentSha: t.OpponentSha,
			OpponentSpec: t.OpponentSpec, Games: t.Games, CreatedAt: t.CreatedAt,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out
}

// lookupTask 读取任务记录（字段在写入后不可变，返回值可安全在锁外读取）。
func (s *Server) lookupTask(taskID string) (*runningTask, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[taskID]
	return t, ok
}

// markTaskDone 标记任务已上报结束（释放 rating 的在飞名额）。
func (s *Server) markTaskDone(taskID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t, ok := s.tasks[taskID]; ok {
		t.Done = true
	}
}

// GetInfo 下发调度器全局信息（trainer 启动时获取变体，worker 零配置）。
//
// 同时下发绝对强度的停机信号：trainer 按间隔轮询本接口，should_stop=true 时走既有
// 优雅停止路径（先把当前轮训完并落 checkpoint 再退出）。
func (s *Server) GetInfo(ctx context.Context, req *pb.GetInfoRequest) (*pb.GetInfoReply, error) {
	return &pb.GetInfoReply{
		Variant:    s.cfg.Variant,
		ShouldStop: s.ctl.ShouldStop(),
		StopReason: s.ctl.StopReason(),
	}, nil
}

// GetTrainConfig 下发运行时可调训练配置（trainer 启动 bootstrap + 运行中热更）。
// 变体不符时拒绝：不同变体的超参与目标语义不同，误下发会静默污染实验。
func (s *Server) GetTrainConfig(ctx context.Context, req *pb.GetTrainConfigRequest) (*pb.GetTrainConfigReply, error) {
	if req.Variant != s.cfg.Variant {
		return &pb.GetTrainConfigReply{
			Message: fmt.Sprintf("变体不符：请求 %q，服务端 %q", req.Variant, s.cfg.Variant),
		}, nil
	}
	return &pb.GetTrainConfigReply{Accepted: true, Overrides: s.ctl.TrainConfig()}, nil
}

// Heartbeat 记录 worker 状态并回传 best 网络与暂停标志。
func (s *Server) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatReply, error) {
	if err := s.store.TouchWorker(ctx, req.WorkerId, int(req.CurrentThreads), int(req.CompletedGames),
		req.ClientVersion, req.MemoryMb); err != nil {
		return nil, fmt.Errorf("touch worker %s: %w", req.WorkerId, err)
	}
	if req.ClientVersion != "" {
		if v, err := s.store.WorkerVersion(ctx, req.WorkerId); err == nil && v != "" && v != req.ClientVersion {
			log.Printf("[worker] %s version changed %s -> %s", req.WorkerId, v, req.ClientVersion)
		}
	}
	best, err := s.store.GetBest(ctx)
	if err != nil {
		return nil, fmt.Errorf("get best network: %w", err)
	}
	reply := &pb.HeartbeatReply{PauseSelfPlay: s.ctl.Paused()}
	if best != nil {
		reply.BestNetwork = best.Sha
	}
	return reply, nil
}
