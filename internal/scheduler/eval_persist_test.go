package scheduler

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"banqi/server/internal/store"
)

// 用 eval 流程验证「重启后队列恢复」端到端契约。
//
// 验证：进程退出前已入队的 (network, spec) 必须能从 eval_queue 表被 New() 拉回
// s.evals，下一个 claim 能继续下发——这正是 P0-C 要修的「重启静默丢评测」。
func TestEvalQueuePersistsAcrossRestart(t *testing.T) {
	ctx := context.Background()
	cfg := Config{
		Variant: "4x8", EvalEnabled: true,
		EvalOpponents: []string{EvalOpponentRandom, EvalOpponentCaptureFirst},
		EvalGames:     1000, EvalEveryNPromotions: 1,
	}
	dbPath := filepath.Join(t.TempDir(), "sched.db")

	// 第一进程：入队 → 不做上报，直接退出。
	st1, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store 1: %v", err)
	}
	srv1 := &Server{cfg: cfg, ctl: &Control{store: st1}, store: st1, tasks: map[string]*runningTask{}}
	if err := srv1.loadEvalQueue(ctx); err != nil {
		t.Fatalf("load 1: %v", err)
	}
	srv1.onNewBest(ctx, "sha-best") // 应入队 random + capture_first
	if srv1.PendingEval() != 2 {
		t.Fatalf("第 1 进程应入队 2 条，实际 %d", srv1.PendingEval())
	}
	st1.Close() // 模拟进程崩溃——DB 落盘

	// 第二进程：必须把上一进程的待办恢复出来。
	st2, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store 2: %v", err)
	}
	defer st2.Close()
	srv2 := &Server{cfg: cfg, ctl: &Control{store: st2}, store: st2, tasks: map[string]*runningTask{}}
	if err := srv2.loadEvalQueue(ctx); err != nil {
		t.Fatalf("load 2: %v", err)
	}
	if srv2.PendingEval() != 2 {
		t.Fatalf("重启后应恢复 2 条待办，实际 %d", srv2.PendingEval())
	}
	// 顺序也应保持（FIFO）
	job, _ := srv2.claimEvalTask(ctx, "w-restart")
	if job == nil || job.Spec != EvalOpponentRandom {
		t.Errorf("重启后队首应为 random，实际 %+v", job)
	}
}

// 入队 → 收尾（finishEvalTask）→ DB 行必须被清理；重启后不会被拉回。
//
// 否则下次重启后同一 (network, spec) 会被重复派发，浪费算力 + 噪声日志。
func TestFinishEvalTaskRemovesDbRow(t *testing.T) {
	ctx := context.Background()
	cfg := Config{Variant: "4x8", EvalEnabled: true, EvalOpponents: []string{EvalOpponentCaptureFirst}, EvalGames: 1000, EvalEveryNPromotions: 1}
	dbPath := filepath.Join(t.TempDir(), "sched.db")

	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()
	srv := &Server{cfg: cfg, ctl: &Control{store: st}, store: st, tasks: map[string]*runningTask{}}
	if err := srv.loadEvalQueue(ctx); err != nil {
		t.Fatalf("load: %v", err)
	}
	srv.onNewBest(ctx, "sha-best")
	job, taskID := srv.claimEvalTask(ctx, "w1")
	if job == nil {
		t.Fatal("应能认领到任务")
	}
	srv.finishEvalTask(ctx, taskID, "sha-best", EvalOpponentCaptureFirst)

	rows, _ := st.LoadEvalQueue(ctx)
	if len(rows) != 0 {
		t.Errorf("finishEvalTask 后 DB 仍残留 %d 条待办（应清零）", len(rows))
	}

	// 假装重启：loadEvalQueue 后内存应空
	srv2 := &Server{cfg: cfg, ctl: &Control{store: st}, store: st, tasks: map[string]*runningTask{}}
	if err := srv2.loadEvalQueue(ctx); err != nil {
		t.Fatalf("load 2: %v", err)
	}
	if srv2.PendingEval() != 0 {
		t.Errorf("重启后不应有遗留待办，实际 %d", srv2.PendingEval())
	}
}

// 重启恢复时 attempts 必须持久化（核心动机：进程反复崩溃后不会让同 (network, spec)
// 永久占住队首）。重启后第一次 claim 仍能拿到，但 attempts 已带上之前的次数。
func TestEvalAttemptsPersistsAcrossRestart(t *testing.T) {
	ctx := context.Background()
	cfg := Config{Variant: "4x8", EvalEnabled: true, EvalOpponents: []string{EvalOpponentCaptureFirst}, EvalGames: 1000, EvalEveryNPromotions: 1}
	dbPath := filepath.Join(t.TempDir(), "sched.db")

	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()
	srv := &Server{cfg: cfg, ctl: &Control{store: st}, store: st, tasks: map[string]*runningTask{}}
	if err := srv.loadEvalQueue(ctx); err != nil {
		t.Fatalf("load: %v", err)
	}
	srv.onNewBest(ctx, "sha-best")
	// 模拟「下发 2 次但都没上报就崩了」
	job1, taskID1 := srv.claimEvalTask(ctx, "w1")
	srv.tasks[taskID1].CreatedAt = time.Now().Add(-2 * evalTaskStaleAfter)
	job2, taskID2 := srv.claimEvalTask(ctx, "w1")
	srv.tasks[taskID2].CreatedAt = time.Now().Add(-2 * evalTaskStaleAfter)
	if job1 == nil || job2 == nil {
		t.Fatalf("应能连发 2 次，实际 %+v %+v", job1, job2)
	}
	if got, _ := st.GetEvalQueueAttempts(ctx, "sha-best", EvalOpponentCaptureFirst); got != 2 {
		t.Errorf("DB attempts 应为 2，实际 %d", got)
	}
	st.Close()

	// 重启
	st2, _ := store.Open(dbPath)
	defer st2.Close()
	srv2 := &Server{cfg: cfg, ctl: &Control{store: st2}, store: st2, tasks: map[string]*runningTask{}}
	if err := srv2.loadEvalQueue(ctx); err != nil {
		t.Fatalf("load: %v", err)
	}
	// 重启后内存 attempts 已是 2：再 claim 一次 → 3 → 下次才丢
	job, _ := srv2.claimEvalTask(ctx, "w1")
	if job == nil {
		t.Fatal("重启后应仍能拿到任务（attempts 没到上限）")
	}
	if job.Attempts != 3 {
		t.Errorf("重启后内存 attempts 应为 3（继承 DB 值），实际 %d", job.Attempts)
	}
}

// 节流计数器 eval_promotions 必须跨重启保留——否则重启会把节流 1/4 误判为 1/4
// 的起点，导致短时间内多发一轮 eval（评测本身幂等，但 log 噪声 + 算力浪费）。
func TestEvalPromotionsCounterPersists(t *testing.T) {
	ctx := context.Background()
	cfg := Config{Variant: "4x8", EvalEnabled: true, EvalOpponents: []string{EvalOpponentCaptureFirst}, EvalGames: 1000, EvalEveryNPromotions: 4}
	dbPath := filepath.Join(t.TempDir(), "sched.db")

	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()
	srv := &Server{cfg: cfg, ctl: &Control{store: st}, store: st, tasks: map[string]*runningTask{}}
	if err := srv.loadEvalQueue(ctx); err != nil {
		t.Fatalf("load: %v", err)
	}
	// 3 次晋级都不触发（1/4）；第 4 次触发
	for i := 0; i < 4; i++ {
		srv.onNewBest(ctx, "sha-best")
	}
	if srv.PendingEval() != 1 {
		t.Errorf("4 次晋级后应入队 1 次（节流 1/4），实际 %d 条", srv.PendingEval())
	}
	if v, _ := st.GetSetting(ctx, evalPromotionsKey); v != "4" {
		t.Errorf("DB 计数器应为 4，实际 %q", v)
	}
	st.Close()

	// 重启
	st2, _ := store.Open(dbPath)
	defer st2.Close()
	srv2 := &Server{cfg: cfg, ctl: &Control{store: st2}, store: st2, tasks: map[string]*runningTask{}}
	if err := srv2.loadEvalQueue(ctx); err != nil {
		t.Fatalf("load: %v", err)
	}
	// 重启后内存 counter=4（继承 DB），再 onNewBest 一次 → counter=5、5%4=1 → 不触发
	srv2.onNewBest(ctx, "sha-best-2")
	if srv2.PendingEval() != 1 {
		t.Errorf("重启后第 1 次晋级不应触发（节流相位继承），实际多出 %d 条", srv2.PendingEval()-1)
	}
	srv2.onNewBest(ctx, "sha-best-3")
	srv2.onNewBest(ctx, "sha-best-4")
	srv2.onNewBest(ctx, "sha-best-5") // counter=8、8%%4=0 → 触发
	if srv2.PendingEval() != 2 {
		t.Errorf("重启后第 4 次晋级应再次触发（counter 8mod4=0），实际 %d 条", srv2.PendingEval())
	}
}
