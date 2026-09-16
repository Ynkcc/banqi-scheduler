package scheduler

import (
	"context"
	"testing"

	pb "banqi/server/pb"
)

func newTestServer(cfg Config) *Server {
	return &Server{cfg: cfg, tasks: map[string]*runningTask{}}
}

// 提交入口的三条拒绝路径（未启用 / 跨变体 / 空载荷）与正常入队。
func TestSubmitReanalysisGateKeeping(t *testing.T) {
	ctx := context.Background()

	disabled := newTestServer(Config{Variant: "4x4", ReanalysisIntervalTasks: 0})
	rep, err := disabled.SubmitReanalysis(ctx, &pb.SubmitReanalysisRequest{Variant: "4x4", Payload: []byte{1}})
	if err != nil {
		t.Fatalf("SubmitReanalysis 不应返回错误: %v", err)
	}
	if rep.Accepted {
		t.Error("未启用重搜（interval=0）时应拒绝提交")
	}

	srv := newTestServer(Config{Variant: "4x4", ReanalysisIntervalTasks: 1, ReanalysisMaxQueue: 2})
	if rep, _ := srv.SubmitReanalysis(ctx, &pb.SubmitReanalysisRequest{Variant: "4x8", Payload: []byte{1}}); rep.Accepted {
		t.Error("跨变体提交应拒绝（防串变体）")
	}
	if rep, _ := srv.SubmitReanalysis(ctx, &pb.SubmitReanalysisRequest{Variant: "4x4"}); rep.Accepted {
		t.Error("空载荷应拒绝")
	}

	rep, _ = srv.SubmitReanalysis(ctx, &pb.SubmitReanalysisRequest{
		Variant: "4x4", Payload: []byte{1, 2}, Positions: 5, Requester: "trainer"})
	if !rep.Accepted {
		t.Fatalf("合法提交应被接受，message=%q", rep.Message)
	}
	if rep.QueuedPositions != 5 {
		t.Errorf("QueuedPositions = %d，期望 5", rep.QueuedPositions)
	}
}

// 队列语义：上限拒绝、按间隔节流、peek 不消费、commit 消费并重置节流。
func TestReanalysisQueueThrottleCapAndConsume(t *testing.T) {
	srv := newTestServer(Config{Variant: "4x2", ReanalysisIntervalTasks: 2, ReanalysisMaxQueue: 1})

	first := &pendingReanalysis{Variant: "4x2", Positions: 3, Payload: []byte{1}}
	if _, ok := srv.enqueueReanalysis(first); !ok {
		t.Fatal("队列未满时应入队成功")
	}
	if _, ok := srv.enqueueReanalysis(&pendingReanalysis{Positions: 1}); ok {
		t.Fatal("队列满（上限 1）时应拒绝新提交，且不丢已有条目")
	}
	if got := srv.peekReanalysisTask(); got != nil {
		t.Fatal("未满 N 个 selfplay 任务前不应下发重搜")
	}
	srv.noteSelfplayAssigned()
	if got := srv.peekReanalysisTask(); got != nil {
		t.Fatal("selfplay 计数 1 < 间隔 2，不应下发")
	}
	srv.noteSelfplayAssigned()
	if got := srv.peekReanalysisTask(); got != first {
		t.Fatal("达到间隔后应下发队首")
	}
	if got := srv.peekReanalysisTask(); got != first {
		t.Fatal("peek 不应消费队首（组装失败要能重试）")
	}
	srv.commitReanalysisTask()
	if len(srv.reanalysis) != 0 {
		t.Fatal("commit 后队首应被消费")
	}

	// commit 重置节流计数：新入队需重新攒够间隔
	srv.enqueueReanalysis(&pendingReanalysis{Variant: "4x2", Positions: 2, Payload: []byte{2}})
	if got := srv.peekReanalysisTask(); got != nil {
		t.Fatal("commit 应重置节流计数，未攒够间隔不应下发")
	}
	srv.noteSelfplayAssigned()
	srv.noteSelfplayAssigned()
	if positions, batches := srv.PendingReanalysis(); positions != 2 || batches != 1 {
		t.Fatalf("PendingReanalysis = (%d, %d)，期望 (2, 1)", positions, batches)
	}
}
