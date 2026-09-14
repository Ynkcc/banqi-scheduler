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

// GetTask 按「未完结 gatekeeper 对打优先、其次 best 网络 selfplay」下发任务。
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
	resp := &pb.TaskResponse{
		TaskId:           taskID,
		Kind:             pb.TaskKind_TASK_SELFPLAY,
		NetworkSha:       best.Sha,
		NetworkShaRemote: best.Sha,
		Games:            s.gamesFor(req.Threads),
		Params:           &pb.SelfPlayParams{Variant: s.cfg.Variant, ExtraConfig: s.extraConfig()},
	}
	if req.CurrentNetwork != best.Sha {
		url, err := s.r2.PresignGet(ctx, r2.NetworkKey(best.Sha))
		if err != nil {
			return nil, fmt.Errorf("presign best network %s: %w", best.Sha, err)
		}
		resp.NetworkUrl = url
	}
	s.registerTask(taskID, &runningTask{Kind: pb.TaskKind_TASK_SELFPLAY, NetworkSha: best.Sha, Games: int(resp.Games), WorkerID: req.WorkerId, CreatedAt: time.Now()})
	log.Printf("[task] selfplay assigned worker=%s task=%s network=%s games=%d", req.WorkerId, taskID, best.Sha, resp.Games)
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
	resp := &pb.TaskResponse{
		TaskId:           taskID,
		Kind:             pb.TaskKind_TASK_RATING,
		NetworkSha:       m.Candidate,
		OpponentSha:      m.Opponent,
		NetworkShaRemote: m.Candidate,
		Games:            int32(games),
		Params:           &pb.SelfPlayParams{Variant: s.cfg.Variant, ExtraConfig: s.extraConfig()},
	}
	candidateURL, err := s.r2.PresignGet(ctx, r2.NetworkKey(m.Candidate))
	if err != nil {
		return nil, fmt.Errorf("presign candidate %s: %w", m.Candidate, err)
	}
	opponentURL, err := s.r2.PresignGet(ctx, r2.NetworkKey(m.Opponent))
	if err != nil {
		return nil, fmt.Errorf("presign opponent %s: %w", m.Opponent, err)
	}
	resp.NetworkUrl = candidateURL
	resp.OpponentUrl = opponentURL
	return resp, nil
}
