package scheduler

import (
	"context"
	"fmt"
	"log"
	"time"

	"banqi/server/internal/r2"
	pb "banqi/server/pb"
)

// 局面重搜（reanalysis）任务队列。
//
// 数据流：trainer 从历史 episode 里攒下局面快照 → SubmitReanalysis 入队 → GetTask 按节流
// 下发给任意 worker → worker 用**当前 best 网络**重跑 MCTS → 产物走常规 ReportEpisode 通道
// 进训练数据。重搜与自对弈争抢同一份算力，因此按 ReanalysisIntervalTasks 节流
// （每 N 个 selfplay 任务最多下发 1 个重搜任务）；SCHEDULER_REANALYSIS_INTERVAL_TASKS=0
// 时整个特性关闭（SubmitReanalysis 直接拒绝，队列不积累）。
//
// 队列上限 ReanalysisMaxQueue 按「载荷条数」计：满了拒绝新提交（不静默丢最旧 —— 最旧的位置
// 恰恰是重搜收益最大的，丢弃顺序不对；拒绝则由 trainer 侧位置池继续持有）。

// pendingReanalysis 是一条待下发的重搜任务（trainer 提交的原始载荷 + 元信息）。
type pendingReanalysis struct {
	Variant   string
	MctsSims  int
	Payload   []byte
	Positions uint32
	Requester string
	CreatedAt time.Time
}

// SubmitReanalysis 接收 trainer 提交的重搜载荷；异步入队，由 GetTask 分发。
func (s *Server) SubmitReanalysis(_ context.Context, req *pb.SubmitReanalysisRequest) (*pb.SubmitReanalysisReply, error) {
	if s.cfg.ReanalysisIntervalTasks <= 0 {
		return &pb.SubmitReanalysisReply{
			Accepted: false,
			Message:  "服务端未启用局面重搜（SCHEDULER_REANALYSIS_INTERVAL_TASKS=0）",
		}, nil
	}
	if req.Variant != s.cfg.Variant {
		return &pb.SubmitReanalysisReply{
			Accepted: false,
			Message:  fmt.Sprintf("变体不一致：请求 %q，服务端 %q（拒绝跨变体重搜）", req.Variant, s.cfg.Variant),
		}, nil
	}
	if len(req.Payload) == 0 {
		return &pb.SubmitReanalysisReply{Accepted: false, Message: "重搜载荷为空"}, nil
	}

	item := &pendingReanalysis{
		Variant:   req.Variant,
		MctsSims:  int(req.MctsSims),
		Payload:   req.Payload,
		Positions: req.Positions,
		Requester: req.Requester,
		CreatedAt: time.Now(),
	}
	queued, ok := s.enqueueReanalysis(item)
	if !ok {
		return &pb.SubmitReanalysisReply{
			Accepted: false,
			Message: fmt.Sprintf("重搜队列已满（上限 %d 批），本次丢弃：请降低提交频率或提高 SCHEDULER_REANALYSIS_MAX_QUEUE",
				s.cfg.ReanalysisMaxQueue),
		}, nil
	}
	log.Printf("[reanalysis] 入队 requester=%s positions=%d sims=%d 队内待重搜位置=%d",
		item.Requester, item.Positions, item.MctsSims, queued)
	return &pb.SubmitReanalysisReply{Accepted: true, QueuedPositions: queued}, nil
}

// enqueueReanalysis 入队；队列满时拒绝（返回 false），不丢弃已有条目。
// 返回入队后的待重搜位置总数。
func (s *Server) enqueueReanalysis(item *pendingReanalysis) (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cfg.ReanalysisMaxQueue > 0 && len(s.reanalysis) >= s.cfg.ReanalysisMaxQueue {
		return s.pendingReanalysisLocked(), false
	}
	s.reanalysis = append(s.reanalysis, item)
	return s.pendingReanalysisLocked(), true
}

// peekReanalysisTask 按节流查看队首待下发的重搜任务（不消费）；不足间隔或无待办时返回 nil。
//
// 与 commitReanalysisTask 分开是为了「组装任务失败时不丢数据」：组装（拉 best / 签发 URL）
// 可能失败，此时保留队首待下次 GetTask 重试，而不是把 trainer 提交的位置丢掉。
func (s *Server) peekReanalysisTask() *pendingReanalysis {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.reanalysis) == 0 || s.reanalysisSinceSelfplay < s.cfg.ReanalysisIntervalTasks {
		return nil
	}
	return s.reanalysis[0]
}

// commitReanalysisTask 消费队首（成功下发后调用），并重置节流计数。
func (s *Server) commitReanalysisTask() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.reanalysis) > 0 {
		s.reanalysis = s.reanalysis[1:]
	}
	s.reanalysisSinceSelfplay = 0
}

// noteSelfplayAssigned 记录一次 selfplay 下发（重搜节流计数）。
func (s *Server) noteSelfplayAssigned() {
	s.mu.Lock()
	s.reanalysisSinceSelfplay++
	s.mu.Unlock()
}

// pendingReanalysisLocked 待重搜位置总数（调用方须持锁）。
func (s *Server) pendingReanalysisLocked() uint64 {
	var total uint64
	for _, it := range s.reanalysis {
		total += uint64(it.Positions)
	}
	return total
}

// PendingReanalysis 待重搜位置总数与批数（WebUI / 日志用）。
func (s *Server) PendingReanalysis() (positions uint64, batches int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pendingReanalysisLocked(), len(s.reanalysis)
}

// reanalysisTask 组装重搜任务响应：用当前 best 网络（重搜的全部意义就是「更强的网络重搜旧局面」）。
// 与 selfplay 一样，对象键恒下发、下载 URL 仅在 worker 本地缺该 sha 时签发。
func (s *Server) reanalysisTask(ctx context.Context, taskID string, item *pendingReanalysis, req *pb.TaskRequest) (*pb.TaskResponse, error) {
	best, err := s.store.GetBest(ctx)
	if err != nil {
		return nil, fmt.Errorf("get best network: %w", err)
	}
	if best == nil {
		return nil, fmt.Errorf("no best network registered")
	}
	networkKey := r2.NetworkKey(best.Sha, best.Format)
	resp := &pb.TaskResponse{
		TaskId:            taskID,
		Kind:              pb.TaskKind_TASK_REANALYSIS,
		NetworkSha:        best.Sha,
		NetworkShaRemote:  best.Sha,
		Games:             int32(item.Positions),
		NetworkKey:        networkKey,
		ReanalysisPayload: item.Payload,
		Params: &pb.SelfPlayParams{
			Variant:  s.cfg.Variant,
			MctsSims: int32(item.MctsSims),
			// 重搜基于 ResNet/MCTS 搜索，恒为 ResNet 类别（与自对弈的 data_kind 全局切换无关）
			DataKind: pb.DataKind_DATA_RESNET,
		},
	}
	if req.CurrentNetwork != best.Sha {
		url, err := s.r2.PresignGet(ctx, networkKey)
		if err != nil {
			return nil, fmt.Errorf("presign best network %s: %w", networkKey, err)
		}
		resp.NetworkUrl = url
	}
	return resp, nil
}
