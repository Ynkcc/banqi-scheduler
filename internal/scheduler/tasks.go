package scheduler

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"time"

	"banqi/server/internal/r2"
	"banqi/server/internal/store"
	pb "banqi/server/pb"
)

func newID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate random id: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// GetTask 按优先级下发任务：
// 未完结 gatekeeper 对打 > 绝对强度评估（TASK_EVAL）> 局面重搜 > best 网络 selfplay。
func (s *Server) GetTask(ctx context.Context, req *pb.TaskRequest) (*pb.TaskResponse, error) {
	s.pruneTasks(time.Now())

	if s.cfg.MinClientVersion != "" && req.ClientVersion != "" && req.ClientVersion < s.cfg.MinClientVersion {
		return &pb.TaskResponse{Kind: pb.TaskKind_TASK_NONE, Message: "client_version_too_old"}, nil
	}
	if req.ClientVersion != "" {
		log.Printf("[task] worker=%s version=%s threads=%d memory_mb=%d", req.WorkerId, req.ClientVersion, req.Threads, req.MemoryMb)
	}

	// 优先下发未完结的 gatekeeper 对打（lczero target_slice 模式）
	m, err := s.store.PendingMatch(ctx)
	if err != nil {
		return nil, fmt.Errorf("pending match: %w", err)
	}
	if m != nil && !s.ratingInFlight(m.ID) {
		taskID, err := newID()
		if err != nil {
			return nil, err
		}
		resp, err := s.ratingTask(ctx, taskID, m, req)
		if err != nil {
			return nil, fmt.Errorf("rating task match=%d: %w", m.ID, err)
		}
		s.registerTask(taskID, &runningTask{Kind: pb.TaskKind_TASK_RATING, MatchID: m.ID,
			NetworkSha: m.Candidate, OpponentSha: m.Opponent, Games: int(resp.Games), WorkerID: req.WorkerId, CreatedAt: time.Now()})
		log.Printf("[task] rating assigned worker=%s match=%d candidate=%s opponent=%s", req.WorkerId, m.ID, m.Candidate, m.Opponent)
		return resp, nil
	}

	if s.ctl.Paused() {
		return &pb.TaskResponse{Kind: pb.TaskKind_TASK_NONE, Message: "self_play_paused"}, nil
	}

	// 绝对强度评估（TASK_EVAL）：best vs 规则/内建对手，只落库、不参与晋级（见 eval.go）。
	// 放在重搜与自对弈之前：单批即含全部局数、成本低（纯策略 1000 局秒级），越早拿到趋势
	// 越能及早发现「练了很久却没变强」——这正是相对门禁结构上测不出来的东西。
	// 受 EvalEnabled / (network,spec) 在飞保护 / 下发次数上限约束；认领是原子的（见 claimEvalTask）。
	if job, taskID := s.claimEvalTask(ctx, req.WorkerId); job != nil {
		resp, err := s.evalTask(ctx, taskID, job, req)
		if err != nil {
			// 组装失败（如网络已被清理）：释放本次认领，队列保留待下轮重试
			s.releaseEvalClaim(taskID)
			log.Printf("[eval] 本次无法下发（%v），已释放认领待重试", err)
		} else {
			log.Printf("[task] eval assigned worker=%s task=%s network=%s opponent=%s games=%d mode=%s",
				req.WorkerId, taskID, resp.NetworkSha, job.Spec, resp.Games, s.evalModeName())
			return resp, nil
		}
	}

	// 局面重搜（reanalysis）：按间隔节流下发，与自对弈共享算力（见 reanalysis.go）。
	// 放在 selfplay 之前 —— 队列里的位置越早重搜收益越大（旧局面 + 当前网络）。
	if item := s.peekReanalysisTask(); item != nil {
		taskID, err := newID()
		if err != nil {
			return nil, err
		}
		resp, err := s.reanalysisTask(ctx, taskID, item, req)
		if err != nil {
			// 组装失败（如暂无 best 网络）：不消费队首，等下次 GetTask 重试
			log.Printf("[reanalysis] 本次无法下发（%v），保留队列待重试", err)
		} else {
			s.commitReanalysisTask()
			s.registerTask(taskID, &runningTask{Kind: pb.TaskKind_TASK_REANALYSIS,
				NetworkSha: resp.NetworkSha, Games: int(item.Positions), WorkerID: req.WorkerId, CreatedAt: time.Now()})
			log.Printf("[task] reanalysis assigned worker=%s task=%s network=%s positions=%d",
				req.WorkerId, taskID, resp.NetworkSha, item.Positions)
			return resp, nil
		}
	}

	// 常规 selfplay：拉 best 网络
	best, err := s.store.GetBest(ctx)
	if err != nil {
		return nil, fmt.Errorf("get best network: %w", err)
	}
	if best == nil {
		return &pb.TaskResponse{Kind: pb.TaskKind_TASK_NONE, Message: "no_best_network_registered"}, nil
	}
	taskID, err := newID()
	if err != nil {
		return nil, err
	}
	// 对象键恒下发（worker 的本地缓存命名依据），下载 URL 仅在需要拉取时签发
	networkKey := r2.NetworkKey(best.Sha, best.Format)
	// 数据类别由调度器全局决定（SCHEDULER_DATA_KIND / WebUI 在线切换），worker 据此产出
	dataKind := s.ctl.DataKind()
	resp := &pb.TaskResponse{
		TaskId:           taskID,
		Kind:             pb.TaskKind_TASK_SELFPLAY,
		NetworkSha:       best.Sha,
		NetworkShaRemote: best.Sha,
		Games:            s.gamesFor(req.Threads),
		Params: &pb.SelfPlayParams{
			Variant:     s.cfg.Variant,
			ExtraConfig: s.extraConfig(),
			DataKind:    dataKind,
		},
		NetworkKey: networkKey,
	}
	if req.CurrentNetwork != best.Sha {
		url, err := s.r2.PresignGet(ctx, networkKey)
		if err != nil {
			return nil, fmt.Errorf("presign best network %s: %w", networkKey, err)
		}
		resp.NetworkUrl = url
	}
	s.noteSelfplayAssigned() // 重搜节流计数
	s.registerTask(taskID, &runningTask{Kind: pb.TaskKind_TASK_SELFPLAY, NetworkSha: best.Sha, Games: int(resp.Games), WorkerID: req.WorkerId, CreatedAt: time.Now()})
	log.Printf("[task] selfplay assigned worker=%s task=%s network=%s games=%d data_kind=%s",
		req.WorkerId, taskID, best.Sha, resp.Games, DataKindName(dataKind))
	return resp, nil
}

// registerTask 将新下发的任务写入内存任务表（归属校验与在飞判定的依据）。
func (s *Server) registerTask(taskID string, t *runningTask) {
	s.mu.Lock()
	s.tasks[taskID] = t
	s.mu.Unlock()
}

// ratingTask 组装 gatekeeper 对打任务：局数取「剩余局数」与「按线程缩放局数」的较小值。
func (s *Server) ratingTask(ctx context.Context, taskID string, m *store.Match, req *pb.TaskRequest) (*pb.TaskResponse, error) {
	remaining := m.TargetGames*2 - m.NumGames
	if remaining <= 0 {
		remaining = 2
	}
	games := remaining
	if scaled := s.gamesFor(req.Threads); int(scaled) < games {
		games = int(scaled)
	}
	// 对象键需按各自权重格式构造（候选与对手可能格式不同）
	candidate, err := s.store.GetNetwork(ctx, m.Candidate)
	if err != nil {
		return nil, fmt.Errorf("get candidate network %s: %w", m.Candidate, err)
	}
	if candidate == nil {
		return nil, fmt.Errorf("candidate network not found sha=%s", m.Candidate)
	}
	opponent, err := s.store.GetNetwork(ctx, m.Opponent)
	if err != nil {
		return nil, fmt.Errorf("get opponent network %s: %w", m.Opponent, err)
	}
	if opponent == nil {
		return nil, fmt.Errorf("opponent network not found sha=%s", m.Opponent)
	}
	candidateKey := r2.NetworkKey(candidate.Sha, candidate.Format)
	opponentKey := r2.NetworkKey(opponent.Sha, opponent.Format)

	resp := &pb.TaskResponse{
		TaskId:           taskID,
		Kind:             pb.TaskKind_TASK_RATING,
		NetworkSha:       m.Candidate,
		OpponentSha:      m.Opponent,
		NetworkShaRemote: m.Candidate,
		Games:            int32(games),
		Params:           &pb.SelfPlayParams{Variant: s.cfg.Variant, ExtraConfig: s.extraConfig()},
		NetworkKey:       candidateKey,
		OpponentKey:      opponentKey,
	}
	candidateURL, err := s.r2.PresignGet(ctx, candidateKey)
	if err != nil {
		return nil, fmt.Errorf("presign candidate %s: %w", candidateKey, err)
	}
	opponentURL, err := s.r2.PresignGet(ctx, opponentKey)
	if err != nil {
		return nil, fmt.Errorf("presign opponent %s: %w", opponentKey, err)
	}
	resp.NetworkUrl = candidateURL
	resp.OpponentUrl = opponentURL
	return resp, nil
}
