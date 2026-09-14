package scheduler

import (
	"context"
	"fmt"
	"log"

	"banqi/server/internal/r2"
	"banqi/server/internal/sprt"
	"banqi/server/internal/store"
	pb "banqi/server/pb"
)

// GetNetwork 下发网络下载信息：sha 为空时取 best。
func (s *Server) GetNetwork(ctx context.Context, req *pb.NetworkRequest) (*pb.NetworkInfo, error) {
	var n *store.Network
	var err error
	if req.Sha == "" {
		n, err = s.store.GetBest(ctx)
	} else {
		n, err = s.store.GetNetwork(ctx, req.Sha)
	}
	if err != nil {
		return nil, fmt.Errorf("get network sha=%q: %w", req.Sha, err)
	}
	if n == nil {
		return nil, fmt.Errorf("network not found sha=%q", req.Sha)
	}
	url, err := s.r2.PresignGet(ctx, r2.NetworkKey(n.Sha))
	if err != nil {
		return nil, fmt.Errorf("presign network %s: %w", n.Sha, err)
	}
	return &pb.NetworkInfo{
		Sha: n.Sha, DownloadUrl: url, CreatedAt: n.CreatedAt.Unix(), IsBest: n.IsBest,
		GameData: fmt.Sprintf(`{"parent_sha":%q}`, n.ParentSha),
	}, nil
}

// RegisterNetwork 登记新网络：首个网络直接晋级，其余创建 gatekeeper 对打。
func (s *Server) RegisterNetwork(ctx context.Context, req *pb.RegisterNetworkRequest) (*pb.RegisterNetworkAck, error) {
	best, err := s.store.GetBest(ctx)
	if err != nil {
		return nil, fmt.Errorf("get best network: %w", err)
	}
	if best != nil && best.Sha == req.Sha {
		return &pb.RegisterNetworkAck{Accepted: false, Message: "sha_equals_current_best"}, nil
	}
	created, err := s.store.RegisterNetwork(ctx, req.Sha, req.ParentSha, req.Notes)
	if err != nil {
		return nil, fmt.Errorf("register network %s: %w", req.Sha, err)
	}
	if !created {
		return &pb.RegisterNetworkAck{Accepted: false, Message: "duplicate_sha:" + req.Sha}, nil
	}
	if best == nil {
		// 首个网络直接晋级
		if err := s.store.PromoteBest(ctx, req.Sha); err != nil {
			return nil, fmt.Errorf("promote first network %s: %w", req.Sha, err)
		}
		log.Printf("[network] first network promoted sha=%s", req.Sha)
		return &pb.RegisterNetworkAck{Accepted: true, Message: "first_network_promoted"}, nil
	}
	matchID, err := s.store.CreateMatch(ctx, req.Sha, best.Sha, s.cfg.GatekeeperGames)
	if err != nil {
		return nil, fmt.Errorf("create match %s vs %s: %w", req.Sha, best.Sha, err)
	}
	log.Printf("[gatekeeper] match=%d created candidate=%s vs best=%s target_pairs=%d",
		matchID, req.Sha, best.Sha, s.cfg.GatekeeperGames)
	return &pb.RegisterNetworkAck{Accepted: true, MatchTaskHint: fmt.Sprintf("gatekeeper match %d vs %s", matchID, best.Sha)}, nil
}

// ReportMatchResult 累计五项成对计数 → GSPRT 判停 → 晋级/拒绝 best 指针。
func (s *Server) ReportMatchResult(ctx context.Context, req *pb.MatchResult) (*pb.MatchResultAck, error) {
	task, ok := s.lookupTask(req.TaskId)
	if !ok {
		return &pb.MatchResultAck{Accepted: false, Message: "unknown_task_id:" + req.TaskId}, nil
	}
	if task.WorkerID != req.WorkerId {
		return &pb.MatchResultAck{Accepted: false, Message: fmt.Sprintf("worker_mismatch task_owner=%s got=%s", task.WorkerID, req.WorkerId)}, nil
	}
	if task.Kind != pb.TaskKind_TASK_RATING {
		return &pb.MatchResultAck{Accepted: false, Message: "task_is_not_rating"}, nil
	}
	// 任务已上报完毕：置 Done 释放 match 的在飞名额（无论本次是否被接受，
	// 重复上报同样应释放，避免 match 卡死）
	s.markTaskDone(req.TaskId)

	m, err := s.store.GetMatch(ctx, task.MatchID)
	if err != nil {
		return nil, fmt.Errorf("get match %d: %w", task.MatchID, err)
	}
	if m == nil || m.Status != "running" {
		return &pb.MatchResultAck{Accepted: false, Message: fmt.Sprintf("match_%d_not_running", task.MatchID)}, nil
	}

	pairs := m.Pairs
	pairs[0] += int(req.PairLl)
	pairs[1] += int(req.PairLd)
	pairs[2] += int(req.PairDd)
	pairs[3] += int(req.PairDw)
	pairs[4] += int(req.PairWw)
	numGames := m.NumGames + int(req.Games)

	verdict, llr, err := s.judgeMatch(m, pairs, numGames)
	if err != nil {
		return nil, err
	}

	status := "running"
	promoted := false
	best, err := s.store.GetBest(ctx)
	if err != nil {
		return nil, fmt.Errorf("get best network: %w", err)
	}
	bestSha := ""
	if best != nil {
		bestSha = best.Sha
	}
	if verdict == sprt.AcceptH1 {
		status = "concluded"
		if err := s.store.PromoteBest(ctx, m.Candidate); err != nil {
			return nil, fmt.Errorf("promote %s: %w", m.Candidate, err)
		}
		promoted = true
		bestSha = m.Candidate
	} else if verdict == sprt.RejectH0 {
		status = "concluded"
		if err := s.store.UpdateNetworkStatus(ctx, m.Candidate, "rejected"); err != nil {
			return nil, fmt.Errorf("reject %s: %w", m.Candidate, err)
		}
	}
	if err := s.store.UpdateMatchResult(ctx, m.ID, pairs, numGames, status); err != nil {
		return nil, fmt.Errorf("update match %d: %w", m.ID, err)
	}
	if verdict != sprt.Continue {
		log.Printf("[gatekeeper] match=%d concluded verdict=%s llr=%.3f pairs=%v promoted=%v", m.ID, verdict, llr, pairs, promoted)
	}
	return &pb.MatchResultAck{Accepted: true, MatchConcluded: verdict != sprt.Continue, Promoted: promoted, BestSha: bestSha}, nil
}

// judgeMatch 判定本局结果是否触发 GSPRT 结论（未达判定条件时返回 Continue）。
func (s *Server) judgeMatch(m *store.Match, pairs [5]int, numGames int) (sprt.Verdict, float64, error) {
	if numGames < m.TargetGames*2 && sprt.Pentanomial(pairs).Total() < m.TargetGames {
		return sprt.Continue, 0, nil
	}
	p := sprt.Pentanomial(pairs)
	bounds := sprt.NewBounds(s.cfg.SprtAlpha, s.cfg.SprtBeta)
	verdict, llr, err := sprt.Judge(p, s.cfg.SprtElo0, s.cfg.SprtElo1, bounds)
	if err != nil {
		return sprt.Continue, 0, fmt.Errorf("sprt judge match=%d: %w", m.ID, err)
	}
	if verdict == sprt.Continue && numGames >= m.TargetGames*2 {
		// 打满目标局数仍无结论：按 LLR 符号判定，LLR>0 视为达标
		if llr > 0 {
			verdict = sprt.AcceptH1
		} else {
			verdict = sprt.RejectH0
		}
		log.Printf("[gatekeeper] match=%d exhausted target_games=%d llr=%.3f forced verdict=%s", m.ID, m.TargetGames, llr, verdict)
	}
	return verdict, llr, nil
}
