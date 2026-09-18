package scheduler

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"banqi/server/internal/store"
	pb "banqi/server/pb"
)

// newEvalTestServer 构造带真实 sqlite 的 Server（判据与落库都需要 store）。
func newEvalTestServer(t *testing.T, cfg Config) (*Server, *store.Store) {
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

func seedEval(t *testing.T, st *store.Store, rows ...store.EvalResult) {
	t.Helper()
	ctx := context.Background()
	for i, r := range rows {
		if r.CreatedAt.IsZero() {
			r.CreatedAt = time.Unix(int64(i+1), 0)
		}
		if err := st.UpsertEvalResult(ctx, r); err != nil {
			t.Fatalf("seed eval %d: %v", i, err)
		}
	}
}

func TestValidateEvalOpponents(t *testing.T) {
	if err := ValidateEvalOpponents(DefaultEvalOpponents); err != nil {
		t.Errorf("默认阶梯应合法: %v", err)
	}
	if err := ValidateEvalOpponents([]string{"", "  ", "random"}); err != nil {
		t.Errorf("空项应忽略: %v", err)
	}
	if err := ValidateEvalOpponents([]string{"random", "rule:capture_first"}); err != nil {
		t.Errorf("合法标识不应报错: %v", err)
	}
	err := ValidateEvalOpponents([]string{"random", "capture_first"})
	if err == nil {
		t.Fatal("缺少 rule: 前缀的非法标识应报错（避免与 collector 解析不一致时静默失败）")
	}
	if err := ValidateEvalOpponents([]string{"network:abc"}); err == nil {
		t.Error("未支持的对手类型应报错")
	}
}

// 入队语义：节流生效、同 (network,spec) 去重。
func TestEvalEnqueueThrottleAndDedup(t *testing.T) {
	ctx := context.Background()
	srv, _ := newEvalTestServer(t, Config{
		EvalEnabled:          true,
		EvalOpponents:        []string{"random", EvalOpponentCaptureFirst},
		EvalGames:            1000,
		EvalEveryNPromotions: 2,
	})

	srv.onNewBest(ctx, "sha1")
	if got := srv.PendingEval(); got != 0 {
		t.Fatalf("节流 1/2：第 1 次晋级不应入队，实际 %d 条", got)
	}
	srv.onNewBest(ctx, "sha2")
	if got := srv.PendingEval(); got != 2 {
		t.Fatalf("第 2 次晋级应入队 2 条（每个对手一条），实际 %d 条", got)
	}
	// 同一网络重复触发（重启补齐 / 重复晋级）不应重复入队
	srv.onNewBest(ctx, "sha2")
	if got := srv.PendingEval(); got != 2 {
		t.Fatalf("同 (network,spec) 应去重，实际 %d 条", got)
	}
}

// 认领语义：原子登记在飞 → 同 (network,spec) 不再认领；超期视为失效可重认领。
func TestEvalClaimIsAtomicAndExcludesInFlight(t *testing.T) {
	ctx := context.Background()
	srv, _ := newEvalTestServer(t, Config{
		EvalEnabled: true, EvalOpponents: []string{"random", EvalOpponentCaptureFirst},
		EvalGames: 1000, EvalEveryNPromotions: 1,
	})
	srv.onNewBest(ctx, "sha1")
	if n := srv.PendingEval(); n != 2 {
		t.Fatalf("应入队 2 条，实际 %d", n)
	}

	job, taskID := srv.claimEvalTask(ctx, "w1")
	if job == nil || job.NetworkSha != "sha1" || job.Spec != "random" {
		t.Fatalf("队首应为 (sha1, random)，实际 %+v", job)
	}
	if taskID == "" {
		t.Fatal("认领应返回任务 id")
	}
	if t2, ok := srv.lookupTask(taskID); !ok || t2.Kind != pb.TaskKind_TASK_EVAL {
		t.Fatalf("认领应同时登记在飞任务，实际 %+v ok=%v", t2, ok)
	}

	// 在飞期间（上报前）不得重复认领同一 (network, spec)
	if j2, _ := srv.claimEvalTask(ctx, "w2"); j2 != nil {
		t.Fatalf("在飞时不应重复认领，实际 %+v", j2)
	}

	// 超过 stale 窗口（worker 掉线）→ 允许重认领（重做同一件事，结果按 (net,spec) 覆盖）
	srv.mu.Lock()
	srv.tasks[taskID].CreatedAt = time.Now().Add(-2 * evalTaskStaleAfter)
	srv.mu.Unlock()
	job2, taskID2 := srv.claimEvalTask(ctx, "w2")
	if job2 == nil || job2.Spec != "random" || taskID2 == taskID {
		t.Fatalf("超期未上报应允许重认领，实际 %+v id=%s", job2, taskID2)
	}
}

// 收尾语义（回归用例）：移出待办队列与释放在飞认领必须同时完成。
//
// 历史事故：上报侧先 `markTaskDone`（释放在飞）再移出队列，两者之间有 DB 往返窗口，
// 窗口内「任务不在飞 + 待办仍在队列」会让 GetTask 重复下发同一 (network, spec)
// —— 实测出现过同一次评测被下发两次、两条结果互相覆盖。
func TestEvalFinishReleasesQueueAndClaimTogether(t *testing.T) {
	ctx := context.Background()
	srv, _ := newEvalTestServer(t, Config{
		EvalEnabled: true, EvalOpponents: []string{EvalOpponentCaptureFirst},
		EvalGames: 1000, EvalEveryNPromotions: 1,
	})
	srv.onNewBest(ctx, "sha1")

	_, taskID := srv.claimEvalTask(ctx, "w1")
	srv.finishEvalTask(ctx, taskID, "sha1", EvalOpponentCaptureFirst)

	if n := srv.PendingEval(); n != 0 {
		t.Errorf("收尾必须同时移出待办队列，实际剩 %d 条", n)
	}
	if t2, ok := srv.lookupTask(taskID); !ok || !t2.Done {
		t.Errorf("收尾必须释放在飞认领（标记 Done），实际 %+v ok=%v", t2, ok)
	}
	// 队列已空 → 不可能再认领到同一任务（旧实现在此处会重复下发）
	if j, _ := srv.claimEvalTask(ctx, "w2"); j != nil {
		t.Fatalf("收尾后不应再认领到任务，实际 %+v", j)
	}
}

// 连续下发未上报达上限：丢弃队首并告警，不阻塞后续任务（防旧版 worker 饿死自对弈）。
func TestEvalDropsTaskAfterMaxAttempts(t *testing.T) {
	ctx := context.Background()
	srv, _ := newEvalTestServer(t, Config{
		EvalEnabled: true, EvalOpponents: []string{"random", EvalOpponentCaptureFirst},
		EvalGames: 1000, EvalEveryNPromotions: 1,
	})
	srv.onNewBest(ctx, "sha1")

	// 队首（random）连续下发 evalMaxAttempts 次均无上报（每次当作超期失效）
	for i := 0; i < evalMaxAttempts; i++ {
		job, taskID := srv.claimEvalTask(ctx, "w")
		if job == nil || job.Spec != "random" {
			t.Fatalf("第 %d 次认领应拿到队首 random，实际 %+v", i+1, job)
		}
		srv.mu.Lock()
		srv.tasks[taskID].CreatedAt = time.Now().Add(-2 * evalTaskStaleAfter)
		srv.mu.Unlock()
	}
	// 达到上限 → 丢弃队首，返回下一条（capture_first）
	next, _ := srv.claimEvalTask(ctx, "w")
	if next == nil || next.Spec != EvalOpponentCaptureFirst {
		t.Fatalf("队首超次数应被丢弃并返回下一条（capture_first），实际 %+v", next)
	}
	if n := srv.PendingEval(); n != 1 {
		t.Fatalf("丢弃后应剩 1 条，实际 %d", n)
	}
}

// 判据：连续 N 次提升不足 → 置位 should_stop；有实质提升 → 不置位。
func TestEvalNoProgressJudgement(t *testing.T) {
	ctx := context.Background()
	cfg := Config{
		EvalEnabled:       true,
		EvalNoProgressN:   3,
		EvalNoProgressEps: 0.02,
	}

	t.Run("停滞触发停机", func(t *testing.T) {
		srv, st := newEvalTestServer(t, cfg)
		seedEval(t, st,
			store.EvalResult{NetworkSha: "v1", OpponentSpec: "rule:capture_first", Wins: 600, Losses: 400, NumGames: 1000},
			store.EvalResult{NetworkSha: "v2", OpponentSpec: "rule:capture_first", Wins: 605, Losses: 395, NumGames: 1000},
			store.EvalResult{NetworkSha: "v3", OpponentSpec: "rule:capture_first", Wins: 607, Losses: 393, NumGames: 1000},
			store.EvalResult{NetworkSha: "v4", OpponentSpec: "rule:capture_first", Wins: 608, Losses: 392, NumGames: 1000},
		)
		srv.judgeEvalProgress(ctx, "rule:capture_first")
		if !srv.ctl.ShouldStop() {
			t.Fatal("连续 3 次提升 < 2pt 应置位 should_stop")
		}
		if r := srv.ctl.StopReason(); r == "" {
			t.Error("停机原因不应为空")
		}
	})

	t.Run("持续提升不触发", func(t *testing.T) {
		srv, st := newEvalTestServer(t, cfg)
		seedEval(t, st,
			store.EvalResult{NetworkSha: "v1", OpponentSpec: "rule:capture_first", Wins: 600, Losses: 400, NumGames: 1000},
			store.EvalResult{NetworkSha: "v2", OpponentSpec: "rule:capture_first", Wins: 650, Losses: 350, NumGames: 1000},
			store.EvalResult{NetworkSha: "v3", OpponentSpec: "rule:capture_first", Wins: 700, Losses: 300, NumGames: 1000},
		)
		srv.judgeEvalProgress(ctx, "rule:capture_first")
		if srv.ctl.ShouldStop() {
			t.Fatalf("每次提升 5pt 不应停机，原因=%q", srv.ctl.StopReason())
		}
	})

	t.Run("关闭判停只观测", func(t *testing.T) {
		srv, st := newEvalTestServer(t, Config{EvalEnabled: true, EvalNoProgressN: 0})
		seedEval(t, st,
			store.EvalResult{NetworkSha: "v1", OpponentSpec: "random", Wins: 500, Losses: 500, NumGames: 1000},
			store.EvalResult{NetworkSha: "v2", OpponentSpec: "random", Wins: 500, Losses: 500, NumGames: 1000},
			store.EvalResult{NetworkSha: "v3", OpponentSpec: "random", Wins: 500, Losses: 500, NumGames: 1000},
			store.EvalResult{NetworkSha: "v4", OpponentSpec: "random", Wins: 500, Losses: 500, NumGames: 1000},
		)
		srv.judgeEvalProgress(ctx, "random")
		if srv.ctl.ShouldStop() {
			t.Error("EvalNoProgressN=0 时应只观测、不置位")
		}
	})
}

// 落库：部分结果不劣化已有结果；更完整的复测覆盖旧值并清退队列。
func TestReportEvalResultPartialDoesNotDegrade(t *testing.T) {
	ctx := context.Background()
	srv, st := newEvalTestServer(t, Config{EvalEnabled: true, EvalNoProgressN: 0})
	task := &runningTask{Kind: pb.TaskKind_TASK_EVAL, NetworkSha: "sha1", OpponentSpec: "rule:capture_first"}
	srv.registerTask("t1", task)
	srv.enqueueEval(ctx, "sha1", []string{"rule:capture_first"})

	full := &pb.MatchResult{
		TaskId: "t1", Kind: pb.TaskKind_TASK_EVAL, NetworkSha: "sha1",
		OpponentSpec: "rule:capture_first", Games: 1000, Wins: 594, Draws: 6, Losses: 400, AvgMoves: 44.4,
	}
	if _, err := srv.reportEvalResult(ctx, task, full); err != nil {
		t.Fatalf("report full: %v", err)
	}
	row, err := st.GetEvalResult(ctx, "sha1", "rule:capture_first")
	if err != nil || row == nil {
		t.Fatalf("get: %v (%v)", row, err)
	}
	if row.NumGames != 1000 || row.Wins != 594 {
		t.Fatalf("落库结果不符: %+v", row)
	}
	if n := srv.PendingEval(); n != 0 {
		t.Fatalf("上报成功应清退队列，实际剩 %d 条", n)
	}

	// 重下发后只带回部分局数：不得覆盖已有完整结果
	partial := &pb.MatchResult{
		TaskId: "t1", Kind: pb.TaskKind_TASK_EVAL, NetworkSha: "sha1",
		OpponentSpec: "rule:capture_first", Games: 100, Wins: 40, Draws: 1, Losses: 59,
	}
	if _, err := srv.reportEvalResult(ctx, task, partial); err != nil {
		t.Fatalf("report partial: %v", err)
	}
	row, _ = st.GetEvalResult(ctx, "sha1", "rule:capture_first")
	if row.NumGames != 1000 {
		t.Errorf("部分结果不应劣化已有结果，实际 games=%d", row.NumGames)
	}

	// 更完整的复测（提高局数）：覆盖
	better := &pb.MatchResult{
		TaskId: "t1", Kind: pb.TaskKind_TASK_EVAL, NetworkSha: "sha1",
		OpponentSpec: "rule:capture_first", Games: 3000, Wins: 1800, Draws: 20, Losses: 1180,
	}
	if _, err := srv.reportEvalResult(ctx, task, better); err != nil {
		t.Fatalf("report better: %v", err)
	}
	row, _ = st.GetEvalResult(ctx, "sha1", "rule:capture_first")
	if row.NumGames != 3000 || row.Wins != 1800 {
		t.Errorf("更完整的复测应覆盖，实际 %+v", row)
	}
}

// 端到端（进程内）：网络晋级 → 入队评估 → GetTask 下发 TASK_EVAL → 上报 → 趋势入库，
// 并验证在飞保护与「评估不占住 selfplay」。
func TestEvalEndToEndDispatchAndReport(t *testing.T) {
	ctx := context.Background()
	srv, st := newEvalTestServer(t, Config{
		Variant: "4x8", EvalEnabled: true,
		EvalOpponents: []string{EvalOpponentCaptureFirst},
		EvalGames:     1000, EvalEveryNPromotions: 1, EvalNoProgressN: 0,
	})

	// 首个网络直接晋级 best → 触发 onNewBest 入队评估
	ack, err := srv.RegisterNetwork(ctx, &pb.RegisterNetworkRequest{Sha: "sha-best", Format: "onnx"})
	if err != nil || !ack.Accepted {
		t.Fatalf("RegisterNetwork: ack=%+v err=%v", ack, err)
	}
	if n := srv.PendingEval(); n != 1 {
		t.Fatalf("晋级后应入队 1 条评估，实际 %d", n)
	}

	// worker 已持有该网络（CurrentNetwork 相同）→ 不应签发下载 URL
	req := &pb.TaskRequest{WorkerId: "w1", ClientVersion: "0.1.0", Threads: 16, CurrentNetwork: "sha-best"}
	resp, err := srv.GetTask(ctx, req)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if resp.Kind != pb.TaskKind_TASK_EVAL {
		t.Fatalf("应下发 TASK_EVAL，实际 %v（message=%q）", resp.Kind, resp.Message)
	}
	if resp.OpponentSpec != EvalOpponentCaptureFirst {
		t.Errorf("OpponentSpec = %q，期望 %q", resp.OpponentSpec, EvalOpponentCaptureFirst)
	}
	if resp.Games != 1000 {
		t.Errorf("Games = %d，期望 1000（一次任务即含全部局数）", resp.Games)
	}
	if resp.NetworkKey == "" {
		t.Error("应下发 network_key（worker 缓存命名的依据）")
	}
	if resp.OpponentKey != "" || resp.OpponentUrl != "" {
		t.Errorf("规则对手不应携带网络键/URL，实际 key=%q url=%q", resp.OpponentKey, resp.OpponentUrl)
	}
	if resp.Params == nil || resp.Params.MctsSims != 0 {
		t.Errorf("默认应下发纯策略（mcts_sims=0），实际 %+v", resp.Params)
	}
	if resp.Params != nil && resp.Params.Variant != "4x8" {
		t.Errorf("变体 = %q，期望 4x8", resp.Params.Variant)
	}

	// 在飞保护：同 (network, spec) 不重复下发，且不阻塞 selfplay
	next, err := srv.GetTask(ctx, req)
	if err != nil {
		t.Fatalf("GetTask 2: %v", err)
	}
	if next.Kind == pb.TaskKind_TASK_EVAL {
		t.Error("评估在飞时不应重复下发同一 (network, spec)")
	}
	if next.Kind != pb.TaskKind_TASK_SELFPLAY {
		t.Errorf("评估在飞时应回落到 selfplay，实际 %v", next.Kind)
	}

	// 上报 → 落库 + 清退队列 + 释放在飞名额
	rep, err := srv.ReportMatchResult(ctx, &pb.MatchResult{
		WorkerId: "w1", TaskId: resp.TaskId, Kind: pb.TaskKind_TASK_EVAL,
		NetworkSha: "sha-best", OpponentSpec: EvalOpponentCaptureFirst,
		Games: 1000, Wins: 576, Draws: 0, Losses: 424, AvgMoves: 44.4,
	})
	if err != nil || !rep.Accepted {
		t.Fatalf("ReportMatchResult: ack=%+v err=%v", rep, err)
	}
	if rep.Promoted {
		t.Error("评估结果不得影响晋级")
	}
	row, err := st.GetEvalResult(ctx, "sha-best", EvalOpponentCaptureFirst)
	if err != nil || row == nil {
		t.Fatalf("评估结果未落库: %v (%v)", row, err)
	}
	if row.Wins != 576 || row.NumGames != 1000 {
		t.Errorf("落库结果不符: %+v", row)
	}
	if n := srv.PendingEval(); n != 0 {
		t.Errorf("上报成功后队列应清空，实际 %d", n)
	}
}

// GetInfo 必须下发布 should_stop（trainer 依此优雅停止）。
func TestGetInfoCarriesShouldStop(t *testing.T) {
	ctx := context.Background()
	srv, _ := newEvalTestServer(t, Config{Variant: "4x8"})
	rep, err := srv.GetInfo(ctx, &pb.GetInfoRequest{})
	if err != nil {
		t.Fatalf("GetInfo: %v", err)
	}
	if rep.Variant != "4x8" || rep.ShouldStop {
		t.Fatalf("初始状态应为 variant=4x8 should_stop=false，实际 %+v", rep)
	}
	if err := srv.ctl.SetStop(ctx, "eval_no_progress: 测试"); err != nil {
		t.Fatalf("SetStop: %v", err)
	}
	rep, _ = srv.GetInfo(ctx, &pb.GetInfoRequest{})
	if !rep.ShouldStop || rep.StopReason == "" {
		t.Fatalf("置位后应下发 should_stop + 原因，实际 %+v", rep)
	}
	if err := srv.ctl.ClearStop(ctx); err != nil {
		t.Fatalf("ClearStop: %v", err)
	}
	if rep, _ = srv.GetInfo(ctx, &pb.GetInfoRequest{}); rep.ShouldStop {
		t.Error("清除后不应再下发 should_stop")
	}
}

// 幂等闸口：ReportMatchResult 收到同 task_id 的二次上报时不应再走任何聚合/落库路径。
// eval 路径已由 finishEvalTask 同步置 Done，重试直接返回「already_reported」，
// 不会重复触发 judgeEvalProgress 把已置位的 should_stop 再发一遍告警。
func TestReportMatchResultIsIdempotentForEval(t *testing.T) {
	ctx := context.Background()
	cfg := Config{
		Variant:              "4x8",
		EvalEnabled:          true,
		EvalOpponents:        []string{EvalOpponentCaptureFirst},
		EvalGames:            1000,
		EvalEveryNPromotions: 1,
		EvalNoProgressN:      3, // 故意打开判据，验证二次上报不会再触发
		EvalNoProgressEps:    0.02,
	}
	srv, _ := newEvalTestServer(t, cfg)
	if _, err := srv.RegisterNetwork(ctx, &pb.RegisterNetworkRequest{Sha: "sha-best", Format: "onnx"}); err != nil {
		t.Fatalf("RegisterNetwork: %v", err)
	}

	// 拉取一个 TASK_EVAL，模拟 worker 拿到任务
	req := &pb.TaskRequest{WorkerId: "w1", ClientVersion: "0.1.0", CurrentNetwork: "sha-best"}
	resp, err := srv.GetTask(ctx, req)
	if err != nil || resp.Kind != pb.TaskKind_TASK_EVAL {
		t.Fatalf("GetTask 应下发 TASK_EVAL，实际 %+v err=%v", resp, err)
	}

	// 首次上报：evaluate 把 should_stop 置位
	first := &pb.MatchResult{
		WorkerId: "w1", TaskId: resp.TaskId, Kind: pb.TaskKind_TASK_EVAL,
		NetworkSha: "sha-best", OpponentSpec: EvalOpponentCaptureFirst,
		Games: 1000, Wins: 576, Draws: 0, Losses: 424, AvgMoves: 44.4,
	}
	rep1, err := srv.ReportMatchResult(ctx, first)
	if err != nil || !rep1.Accepted {
		t.Fatalf("首次上报：ack=%+v err=%v", rep1, err)
	}
	if srv.ctl.ShouldStop() {
		t.Fatal("单次评估不应触发判据（仅 1 条记录，无对照）")
	}

	// 二次上报：必须命中幂等闸口，不能再次触达 judgeEvalProgress / UpsertEvalResult
	stopReason := srv.ctl.StopReason()
	dup := &pb.MatchResult{
		WorkerId: "w1", TaskId: resp.TaskId, Kind: pb.TaskKind_TASK_EVAL,
		NetworkSha: "sha-best", OpponentSpec: EvalOpponentCaptureFirst,
		// 用「不一样的局数」试探：若没命中闸口，UpsertEvalResult 会用新 created_at 覆盖旧行
		Games: 5000, Wins: 3000, Draws: 0, Losses: 2000, AvgMoves: 50.0,
	}
	rep2, err := srv.ReportMatchResult(ctx, dup)
	if err != nil || !rep2.Accepted {
		t.Fatalf("二次上报应返回 Accepted（幂等）：ack=%+v err=%v", rep2, err)
	}
	if !strings.Contains(rep2.Message, "already_reported") {
		t.Errorf("二次上报应带 already_reported 标记，实际 message=%q", rep2.Message)
	}
	if srv.ctl.StopReason() != stopReason {
		t.Errorf("二次上报不得覆盖 stop_reason：旧=%q 新=%q", stopReason, srv.ctl.StopReason())
	}
}

// 顺序契约：UpsertEvalResult 必须在 finishEvalTask 之前，否则 Done=true 但 DB 未写，
// 同 task_id 重试会被入口闸口丢成「已上报」造成数据丢失。
func TestEvalUpsertBeforeFinishPreventsDataLossOnRetry(t *testing.T) {
	ctx := context.Background()
	srv, _ := newEvalTestServer(t, Config{
		Variant: "4x8", EvalEnabled: true,
		EvalOpponents: []string{EvalOpponentCaptureFirst},
		EvalGames:     1000, EvalEveryNPromotions: 1, EvalNoProgressN: 0,
	})
	if _, err := srv.RegisterNetwork(ctx, &pb.RegisterNetworkRequest{Sha: "sha-best", Format: "onnx"}); err != nil {
		t.Fatalf("RegisterNetwork: %v", err)
	}

	req := &pb.TaskRequest{WorkerId: "w1", ClientVersion: "0.1.0", CurrentNetwork: "sha-best"}
	resp, err := srv.GetTask(ctx, req)
	if err != nil || resp.Kind != pb.TaskKind_TASK_EVAL {
		t.Fatalf("GetTask 应下发 TASK_EVAL，实际 %+v err=%v", resp, err)
	}

	// 模拟「首次上报成功 + 同 task_id 重试」：重试在 entry 闸口命中 Done 短路
	rep := &pb.MatchResult{
		WorkerId: "w1", TaskId: resp.TaskId, Kind: pb.TaskKind_TASK_EVAL,
		NetworkSha: "sha-best", OpponentSpec: EvalOpponentCaptureFirst,
		Games: 1000, Wins: 576, Draws: 0, Losses: 424, AvgMoves: 44.4,
	}
	if _, err := srv.ReportMatchResult(ctx, rep); err != nil {
		t.Fatalf("首次上报失败: %v", err)
	}
	if rep2, err := srv.ReportMatchResult(ctx, rep); err != nil || !rep2.Accepted {
		t.Fatalf("重试应被接受：ack=%+v err=%v", rep2, err)
	} else if !strings.Contains(rep2.Message, "already_reported") {
		t.Errorf("重试应带 already_reported 标记，实际 message=%q", rep2.Message)
	}
	// 数据必须已落库：证明 UpsertEvalResult 在 Done=true 之前执行
	row, err := srv.store.GetEvalResult(ctx, "sha-best", EvalOpponentCaptureFirst)
	if err != nil || row == nil {
		t.Fatalf("数据丢失：GetEvalResult 返回 %+v err=%v", row, err)
	}
	if row.NumGames != 1000 || row.Wins != 576 {
		t.Errorf("落库数据被重试覆盖：%+v", row)
	}
}
