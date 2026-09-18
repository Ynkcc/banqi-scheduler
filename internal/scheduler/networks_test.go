package scheduler

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"banqi/server/internal/store"
	pb "banqi/server/pb"
)

// newRatingTestServer 构造带真实 sqlite + 已就位两个网络的 Server（gatekeeper 路径
// 必然走 match，区别于 eval 测试用单网络直接晋级）。
func newRatingTestServer(t *testing.T, cfg Config) (*Server, *store.Store) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "sched.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return &Server{
		cfg:   cfg,
		ctl:   &Control{store: st},
		store: st,
		tasks: map[string]*runningTask{},
	}, st
}

// 幂等闸口（rating 路径）：同 task_id 的二次上报必须短路，避免把 pairs 重复累加
// —— 否则一旦发生会让 gatekeeper 胜率被无声推高、错误晋级 best。
//
// 测试同时验证短路发生在 DB 写入之前：若短路靠的是「重复 UpsertEvalResult 看不出问题」，
// 对 rating 就不成立——UpdateMatchResult 是累加语句，二次执行会让 pairs 加倍。
func TestReportMatchResultIsIdempotentForRating(t *testing.T) {
	ctx := context.Background()
	srv, st := newRatingTestServer(t, Config{
		Variant:         "4x8",
		GatekeeperGames: 4,
		SprtElo0:        0, SprtElo1: 30, SprtAlpha: 0.05, SprtBeta: 0.05,
	})

	// 登记两个网络 + 一个手动 match（跳过 RegisterNetwork 的自动 match，便于控制 task_id）
	if _, err := st.RegisterNetwork(ctx, "sha-best", "", "best", "onnx"); err != nil {
		t.Fatalf("register best: %v", err)
	}
	if _, err := st.RegisterNetwork(ctx, "sha-cand", "", "cand", "onnx"); err != nil {
		t.Fatalf("register cand: %v", err)
	}
	if err := st.PromoteBest(ctx, "sha-best"); err != nil {
		t.Fatalf("promote best: %v", err)
	}
	matchID, err := st.CreateMatch(ctx, "sha-cand", "sha-best", 4)
	if err != nil {
		t.Fatalf("create match: %v", err)
	}

	// 模拟 GetTask 已下发 rating 任务
	srv.tasks["task-A"] = &runningTask{
		Kind: pb.TaskKind_TASK_RATING, MatchID: matchID,
		NetworkSha: "sha-cand", OpponentSha: "sha-best",
		WorkerID: "w1", Games: 2,
	}

	req := &pb.MatchResult{
		WorkerId: "w1", TaskId: "task-A", Kind: pb.TaskKind_TASK_RATING,
		NetworkSha: "sha-cand", OpponentSha: "sha-best",
		Games:  2,
		PairLl: 1, PairLd: 0, PairDd: 1, PairDw: 0, PairWw: 0,
	}
	rep1, err := srv.ReportMatchResult(ctx, req)
	if err != nil || !rep1.Accepted {
		t.Fatalf("首次上报应接受：ack=%+v err=%v", rep1, err)
	}
	m1, err := st.GetMatch(ctx, matchID)
	if err != nil || m1 == nil {
		t.Fatalf("read match: %+v err=%v", m1, err)
	}
	wantPairs := [5]int{1, 0, 1, 0, 0}
	if m1.Pairs != wantPairs || m1.NumGames != 2 {
		t.Fatalf("首次上报后 match 状态不符：pairs=%v num_games=%d", m1.Pairs, m1.NumGames)
	}

	// 二次上报：必须命中幂等闸口，pairs 不得加倍
	rep2, err := srv.ReportMatchResult(ctx, req)
	if err != nil || !rep2.Accepted {
		t.Fatalf("二次上报应被接受：ack=%+v err=%v", rep2, err)
	}
	if !strings.Contains(rep2.Message, "already_reported") {
		t.Errorf("二次上报应带 already_reported 标记，实际 message=%q", rep2.Message)
	}
	m2, _ := st.GetMatch(ctx, matchID)
	if m2.Pairs != wantPairs || m2.NumGames != 2 {
		t.Fatalf("二次上报不应改 match：pairs=%v num_games=%d", m2.Pairs, m2.NumGames)
	}

	// worker_id 不匹配场景应优先于幂等闸口被拒（防止恶意 worker 借 Done 探测他人任务）
	req3 := *req
	req3.WorkerId = "w2-evil"
	rep3, err := srv.ReportMatchResult(ctx, &req3)
	if err != nil {
		t.Fatalf("worker_mismatch 不应返回错误: %v", err)
	}
	if rep3.Accepted {
		t.Error("worker_mismatch 应拒绝而非命中幂等闸口")
	}
}
