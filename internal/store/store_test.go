package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
)

// TestMigrateFromLegacySchema 覆盖老库升级：先按「无 kind / format 列」的历史建表，
// 再 Open()——补列与依赖新列的索引都必须成功（索引曾建在补列之前，会让老库直接打不开）。
func TestMigrateFromLegacySchema(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "legacy.db")

	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	legacy := []string{
		`CREATE TABLE networks (sha TEXT PRIMARY KEY, parent_sha TEXT, created_at INTEGER NOT NULL,
			is_best INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'candidate', notes TEXT)`,
		`CREATE TABLE episodes (id INTEGER PRIMARY KEY AUTOINCREMENT, worker_id TEXT NOT NULL,
			task_id TEXT NOT NULL, network_sha TEXT NOT NULL, game_count INTEGER NOT NULL,
			total_steps INTEGER NOT NULL, winner INTEGER NOT NULL, object_key TEXT NOT NULL,
			created_at INTEGER NOT NULL)`,
		`CREATE TABLE workers (id TEXT PRIMARY KEY, last_seen INTEGER NOT NULL,
			threads INTEGER NOT NULL DEFAULT 0, completed_games INTEGER NOT NULL DEFAULT 0)`,
		`INSERT INTO networks (sha, created_at, is_best) VALUES ('sha-old', 1, 1)`,
		`INSERT INTO episodes (worker_id, task_id, network_sha, game_count, total_steps, winner, object_key, created_at)
			VALUES ('w0', 't0', 'sha-old', 2, 40, 1, 'episodes/sha-old/legacy.epb.gz', 1)`,
	}
	for _, q := range legacy {
		if _, err := raw.ExecContext(ctx, q); err != nil {
			t.Fatalf("legacy schema %q: %v", q, err)
		}
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close raw: %v", err)
	}

	s, err := Open(path)
	if err != nil {
		t.Fatalf("老库升级失败: %v", err)
	}
	defer s.Close()

	// 旧 episode 行的 kind 落到默认值 0（ResNet）
	keys, err := s.ListEpisodeKeys(ctx, "", 10, 0)
	if err != nil || len(keys) != 1 {
		t.Fatalf("旧库 ResNet 类别应能列出历史对象: %v err=%v", keys, err)
	}
	if keys, err = s.ListEpisodeKeys(ctx, "", 10, 1); err != nil || len(keys) != 0 {
		t.Fatalf("旧库不应有 NNUE 类别记录: %v err=%v", keys, err)
	}
	if n, err := s.GetNetwork(ctx, "sha-old"); err != nil || n == nil || n.Format != "onnx" {
		t.Fatalf("旧网络应补上默认 format=onnx: %+v err=%v", n, err)
	}
}

func TestMigrateIsIdempotent(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "scheduler.db")

	s, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	// 二次打开走旧库升级路径（workers 列已存在），不得报错。
	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	for _, col := range []string{"client_version", "memory_mb"} {
		ok, err := s2.columnExists(ctx, "workers", col)
		if err != nil {
			t.Fatalf("columnExists(%s): %v", col, err)
		}
		if !ok {
			t.Errorf("workers.%s 应存在", col)
		}
	}
	ok, err := s2.columnExists(ctx, "workers", "no_such_column")
	if err != nil {
		t.Fatalf("columnExists(no_such_column): %v", err)
	}
	if ok {
		t.Error("不存在的列不应报告存在")
	}
}

func TestRegisterNetworkAndSettings(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "scheduler.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	created, err := s.RegisterNetwork(ctx, "sha-a", "", "n1", "onnx")
	if err != nil || !created {
		t.Fatalf("首次登记应创建: created=%v err=%v", created, err)
	}
	created, err = s.RegisterNetwork(ctx, "sha-a", "", "dup", "onnx")
	if err != nil {
		t.Fatalf("重复登记不应报错: %v", err)
	}
	if created {
		t.Error("重复 sha 不应再创建")
	}

	if err := s.SetSetting(ctx, "k", "v"); err != nil {
		t.Fatalf("set setting: %v", err)
	}
	if v, err := s.GetSetting(ctx, "k"); err != nil || v != "v" {
		t.Fatalf("get setting: %q err=%v", v, err)
	}
	if v, err := s.GetSetting(ctx, "missing"); err != nil || v != "" {
		t.Fatalf("缺失键应返回空串: %q err=%v", v, err)
	}
}

func TestMatchesEpisodesWorkersRoundTrip(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "scheduler.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	// matches 对 networks 有外键约束，先登记双方网络
	for _, sha := range []string{"sha-cand", "sha-best"} {
		if _, err := s.RegisterNetwork(ctx, sha, "", "", "onnx"); err != nil {
			t.Fatalf("register %s: %v", sha, err)
		}
	}

	matchID, err := s.CreateMatch(ctx, "sha-cand", "sha-best", 4)
	if err != nil {
		t.Fatalf("create match: %v", err)
	}
	m, err := s.PendingMatch(ctx)
	if err != nil || m == nil {
		t.Fatalf("pending match: %+v err=%v", m, err)
	}
	if m.ID != matchID || m.Status != "running" || m.TargetGames != 4 {
		t.Fatalf("unexpected pending match %+v", m)
	}

	if err := s.UpdateMatchResult(ctx, matchID, [5]int{1, 0, 2, 0, 1}, 2, "concluded"); err != nil {
		t.Fatalf("update match: %v", err)
	}
	got, err := s.GetMatch(ctx, matchID)
	if err != nil || got == nil {
		t.Fatalf("get match: %+v err=%v", got, err)
	}
	if got.Status != "concluded" || got.NumGames != 2 || got.Pairs != [5]int{1, 0, 2, 0, 1} {
		t.Fatalf("unexpected match after update: %+v", got)
	}
	if m2, err := s.PendingMatch(ctx); err != nil || m2 != nil {
		t.Fatalf("已完结 match 不应再被 pending: %+v err=%v", m2, err)
	}

	if err := s.InsertEpisode(ctx, Episode{WorkerID: "w1", TaskID: "t1", NetworkSha: "sha-best",
		GameCount: 3, TotalSteps: 99, Winner: 1, ObjectKey: "episodes/sha-best/a.epb.gz"}); err != nil {
		t.Fatalf("insert episode: %v", err)
	}
	// 第二条为 NNUE 类别：验证按类别过滤时游标仍按登记顺序推进
	if err := s.InsertEpisode(ctx, Episode{WorkerID: "w1", TaskID: "t2", NetworkSha: "sha-best",
		GameCount: 5, TotalSteps: 120, Winner: -1, ObjectKey: "episodes/sha-best/b.epb.gz", Kind: 1}); err != nil {
		t.Fatalf("insert nnue episode: %v", err)
	}
	eps, err := s.ListEpisodes(ctx, 0, 10)
	if err != nil || len(eps) != 2 || eps[0].GameCount != 5 {
		t.Fatalf("list episodes: %+v err=%v", eps, err)
	}
	keys, err := s.ListEpisodeKeys(ctx, "", 10, KindAny)
	if err != nil || len(keys) != 2 {
		t.Fatalf("list episode keys: %v err=%v", keys, err)
	}
	// 只取 ResNet 类别：NNUE 对象不出现，且游标仍可正常推进到末尾
	keys, err = s.ListEpisodeKeys(ctx, "", 10, 0)
	if err != nil || len(keys) != 1 || keys[0] != "episodes/sha-best/a.epb.gz" {
		t.Fatalf("按类别过滤应只返回 ResNet 对象: %v err=%v", keys, err)
	}
	keys, err = s.ListEpisodeKeys(ctx, keys[0], 10, 0)
	if err != nil || len(keys) != 0 {
		t.Fatalf("游标应推进到末尾: %v err=%v", keys, err)
	}
	keys, err = s.ListEpisodeKeys(ctx, "", 10, 1)
	if err != nil || len(keys) != 1 || keys[0] != "episodes/sha-best/b.epb.gz" {
		t.Fatalf("按类别过滤应只返回 NNUE 对象: %v err=%v", keys, err)
	}

	if err := s.TouchWorker(ctx, "w1", 8, 42, "v1.2.3", 1024); err != nil {
		t.Fatalf("touch worker: %v", err)
	}
	if v, err := s.WorkerVersion(ctx, "w1"); err != nil || v != "v1.2.3" {
		t.Fatalf("worker version: %q err=%v", v, err)
	}
	ws, err := s.ListWorkers(ctx)
	if err != nil || len(ws) != 1 || ws[0].Threads != 8 || ws[0].CompletedGames != 42 {
		t.Fatalf("list workers: %+v err=%v", ws, err)
	}

	c, err := s.Counts(ctx)
	if err != nil {
		t.Fatalf("counts: %v", err)
	}
	if c.Networks != 2 || c.Episodes != 2 || c.EpisodeGames != 8 || c.Workers != 1 || c.MatchesRunning != 0 {
		t.Fatalf("unexpected counts %+v", c)
	}
}
