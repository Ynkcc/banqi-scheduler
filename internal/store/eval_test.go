package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

func evalRow(sha, spec string, wins, draws, losses int, ts int64) EvalResult {
	return EvalResult{
		NetworkSha:   sha,
		OpponentSpec: spec,
		Wins:         wins,
		Draws:        draws,
		Losses:       losses,
		NumGames:     wins + draws + losses,
		AvgMoves:     44.4,
		CreatedAt:    time.Unix(ts, 0),
	}
}

// TestEvalResultUpsertIsPerSpecAndOverwrites 覆盖「同版本重复评估覆盖、不产生重复点」。
func TestEvalResultUpsertIsPerSpecAndOverwrites(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "sched.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	if err := s.UpsertEvalResult(ctx, evalRow("sha1", "rule:capture_first", 594, 6, 400, 100)); err != nil {
		t.Fatalf("upsert 1: %v", err)
	}
	// 同 (network, spec) 复测：局数提高后覆盖，不新增行
	if err := s.UpsertEvalResult(ctx, evalRow("sha1", "rule:capture_first", 1800, 20, 1180, 200)); err != nil {
		t.Fatalf("upsert overwrite: %v", err)
	}
	// 同网络、不同对手：另一行
	if err := s.UpsertEvalResult(ctx, evalRow("sha1", "random", 960, 10, 30, 101)); err != nil {
		t.Fatalf("upsert other spec: %v", err)
	}

	all, err := s.ListEvalResults(ctx, 0)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("应按 (network, spec) 去重为 2 行，实际 %d 行: %+v", len(all), all)
	}

	got, err := s.GetEvalResult(ctx, "sha1", "rule:capture_first")
	if err != nil || got == nil {
		t.Fatalf("get: %v (%v)", got, err)
	}
	if got.NumGames != 3000 || got.Wins != 1800 {
		t.Errorf("复测应覆盖旧值，实际 games=%d wins=%d（期望 3000 / 1800）", got.NumGames, got.Wins)
	}
}

// TestEvalTrendIsPerSpecChronological 覆盖趋势序列：只含指定对手、按写入顺序升序。
func TestEvalTrendIsPerSpecChronological(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "sched.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	// v1 57.6% → v2 60.0% → v3 60.5%（提升 2.4pt 后仅 0.5pt）
	for i, r := range []EvalResult{
		evalRow("sha1", "rule:capture_first", 576, 0, 424, 1),
		evalRow("sha2", "rule:capture_first", 600, 0, 400, 2),
		evalRow("sha3", "rule:capture_first", 605, 0, 395, 3),
		// 干扰项：另一对手的评估不得进入本对手的趋势
		evalRow("sha2", "random", 950, 0, 50, 2),
		evalRow("sha3", "random", 960, 0, 40, 3),
	} {
		if err := s.UpsertEvalResult(ctx, r); err != nil {
			t.Fatalf("upsert %d: %v", i, err)
		}
	}

	trend, err := s.EvalTrend(ctx, "rule:capture_first", 10)
	if err != nil {
		t.Fatalf("trend: %v", err)
	}
	if len(trend) != 3 {
		t.Fatalf("趋势应含 3 个版本，实际 %d: %+v", len(trend), trend)
	}
	wantShas := []string{"sha1", "sha2", "sha3"}
	for i, r := range trend {
		if r.NetworkSha != wantShas[i] {
			t.Errorf("趋势应升序，位置 %d = %s，期望 %s", i, r.NetworkSha, wantShas[i])
		}
		if r.OpponentSpec != "rule:capture_first" {
			t.Errorf("趋势混入了其他对手: %+v", r)
		}
	}
	if wr := trend[0].WinRate(); wr < 0.5759 || wr > 0.5761 {
		t.Errorf("WinRate = %v，期望约 0.576", wr)
	}

	// limit 取最近 N 个（仍升序）
	last2, err := s.EvalTrend(ctx, "rule:capture_first", 2)
	if err != nil {
		t.Fatalf("trend limit: %v", err)
	}
	if len(last2) != 2 || last2[0].NetworkSha != "sha2" || last2[1].NetworkSha != "sha3" {
		t.Errorf("limit=2 应返回最近两版且升序，实际 %+v", last2)
	}

	opps, err := s.ListEvalOpponents(ctx)
	if err != nil {
		t.Fatalf("opponents: %v", err)
	}
	if len(opps) != 2 || opps[0] != "rule:capture_first" || opps[1] != "random" {
		t.Errorf("对手列表 = %v，期望 [rule:capture_first random]（按首次出现）", opps)
	}
}

// TestEvalResultsTableCreatedOnLegacyDB 覆盖老库升级：旧库没有 eval_results，
// Open 必须补建该表（无外键），且既有数据不受影响。
func TestEvalResultsTableCreatedOnLegacyDB(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "legacy.db")

	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	if _, err := raw.ExecContext(ctx, `CREATE TABLE networks (
		sha TEXT PRIMARY KEY, parent_sha TEXT, created_at INTEGER NOT NULL,
		is_best INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'candidate', notes TEXT)`); err != nil {
		t.Fatalf("legacy schema: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close raw: %v", err)
	}

	s, err := Open(path)
	if err != nil {
		t.Fatalf("老库升级失败: %v", err)
	}
	defer s.Close()

	if err := s.UpsertEvalResult(ctx, evalRow("sha-old", "rule:reveal_first", 987, 0, 13, 1)); err != nil {
		t.Fatalf("老库应可用 eval_results: %v", err)
	}
	trend, err := s.EvalTrend(ctx, "rule:reveal_first", 5)
	if err != nil || len(trend) != 1 {
		t.Fatalf("trend = %+v err=%v", trend, err)
	}
	if c, err := s.Counts(ctx); err != nil || c.EvalResults != 1 {
		t.Errorf("Counts.EvalResults = %d err=%v，期望 1", c.EvalResults, err)
	}
}
