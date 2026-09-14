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
	Kind        pb.TaskKind
	MatchID     int64
	NetworkSha  string
	OpponentSha string
	Games       int
	WorkerID    string
	CreatedAt   time.Time
	// Done 标记任务已上报结束（rating 用）：Done 后即释放 match 的在飞名额。
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
	TaskID      string
	WorkerID    string
	Kind        pb.TaskKind
	MatchID     int64
	NetworkSha  string
	OpponentSha string
	Games       int
	CreatedAt   time.Time
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
}

type Server struct {
	pb.UnimplementedSchedulerServiceServer
	cfg   Config
	ctl   *Control
	store *store.Store
	r2    *r2.Presigner

	mu        sync.Mutex
	tasks     map[string]*runningTask
	lastPrune time.Time
}

func New(ctx context.Context, cfg Config, st *store.Store, presigner *r2.Presigner) (*Server, error) {
	ctl, err := loadControl(ctx, st, cfg.InitialRevealed)
	if err != nil {
		return nil, fmt.Errorf("load control: %w", err)
	}
	return &Server{cfg: cfg, ctl: ctl, store: st, r2: presigner, tasks: make(map[string]*runningTask)}, nil
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
			NetworkSha: t.NetworkSha, OpponentSha: t.OpponentSha, Games: t.Games, CreatedAt: t.CreatedAt,
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
func (s *Server) GetInfo(ctx context.Context, req *pb.GetInfoRequest) (*pb.GetInfoReply, error) {
	return &pb.GetInfoReply{Variant: s.cfg.Variant}, nil
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
