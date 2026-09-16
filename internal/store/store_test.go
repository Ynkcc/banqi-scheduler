package store

import (
	"context"
	"path/filepath"
	"testing"
)

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
	eps, err := s.ListEpisodes(ctx, 0, 10)
	if err != nil || len(eps) != 1 || eps[0].GameCount != 3 {
		t.Fatalf("list episodes: %+v err=%v", eps, err)
	}
	keys, err := s.ListEpisodeKeys(ctx, "", 10)
	if err != nil || len(keys) != 1 {
		t.Fatalf("list episode keys: %v err=%v", keys, err)
	}
	keys, err = s.ListEpisodeKeys(ctx, keys[0], 10)
	if err != nil || len(keys) != 0 {
		t.Fatalf("游标应推进到末尾: %v err=%v", keys, err)
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
	if c.Networks != 2 || c.Episodes != 1 || c.EpisodeGames != 3 || c.Workers != 1 || c.MatchesRunning != 0 {
		t.Fatalf("unexpected counts %+v", c)
	}
}
