package scheduler

import (
	"testing"
	"time"

	pb "banqi/server/pb"
)

func TestPruneTasks(t *testing.T) {
	now := time.Now()
	srv := &Server{tasks: map[string]*runningTask{
		"done-old":   {Kind: pb.TaskKind_TASK_RATING, Done: true, CreatedAt: now.Add(-2 * time.Hour)},
		"done-new":   {Kind: pb.TaskKind_TASK_RATING, Done: true, CreatedAt: now},
		"undone-new": {Kind: pb.TaskKind_TASK_SELFPLAY, CreatedAt: now},
		"undone-old": {Kind: pb.TaskKind_TASK_SELFPLAY, CreatedAt: now.Add(-48 * time.Hour)},
	}}
	srv.pruneTasks(now)

	if _, ok := srv.tasks["done-old"]; ok {
		t.Error("done-old 超出 doneTaskRetention，应被回收")
	}
	if _, ok := srv.tasks["undone-old"]; ok {
		t.Error("undone-old 超出 taskMaxRetention，应被回收")
	}
	if _, ok := srv.tasks["done-new"]; !ok {
		t.Error("done-new 未超 doneTaskRetention，应保留")
	}
	if _, ok := srv.tasks["undone-new"]; !ok {
		t.Error("undone-new 未超 taskMaxRetention，应保留")
	}

	// 节流：pruneInterval 内再次调用不应扫描
	srv.tasks["x"] = &runningTask{CreatedAt: now.Add(-48 * time.Hour)}
	srv.pruneTasks(now.Add(time.Second))
	if _, ok := srv.tasks["x"]; !ok {
		t.Error("pruneInterval 内不应再次扫描回收")
	}
}

func TestRunningTasksExcludesDone(t *testing.T) {
	now := time.Now()
	srv := &Server{tasks: map[string]*runningTask{
		"a": {Kind: pb.TaskKind_TASK_SELFPLAY, CreatedAt: now},
		"b": {Kind: pb.TaskKind_TASK_RATING, Done: true, CreatedAt: now},
	}}
	out := srv.RunningTasks()
	if len(out) != 1 || out[0].TaskID != "a" {
		t.Fatalf("RunningTasks 应只含未结束任务，实际 %+v", out)
	}
}

func TestRatingInFlightStale(t *testing.T) {
	now := time.Now()
	srv := &Server{tasks: map[string]*runningTask{
		"fresh": {Kind: pb.TaskKind_TASK_RATING, MatchID: 7, CreatedAt: now.Add(-time.Minute)},
	}}
	if !srv.ratingInFlight(7) {
		t.Fatal("新鲜 rating 任务应视为在飞")
	}
	srv.tasks["fresh"].CreatedAt = now.Add(-2 * ratingTaskStaleAfter)
	if srv.ratingInFlight(7) {
		t.Fatal("超期 rating 任务不应视为在飞（worker 掉线）")
	}
}

func TestGamesForScaling(t *testing.T) {
	cases := []struct {
		baseline, games, threads, want int32
	}{
		{0, 16, 8, 16},  // 未配置 baseline：不缩放
		{8, 16, 8, 16},  // 与基准一致
		{8, 16, 16, 32}, // 双倍线程
		{8, 16, 4, 8},   // 半线程
		{8, 16, 0, 2},   // threads<=0 视为 1
		{8, 1, 1, 1},    // 向下取整为 0，钳到 1
	}
	for _, c := range cases {
		srv := &Server{cfg: Config{GamesPerTask: int(c.games), ThreadsBaseline: int(c.baseline)}}
		if got := srv.gamesFor(c.threads); got != c.want {
			t.Errorf("gamesFor(baseline=%d, games=%d, threads=%d) = %d, want %d",
				c.baseline, c.games, c.threads, got, c.want)
		}
	}
}

func TestExtraConfig(t *testing.T) {
	srv := &Server{ctl: &Control{}}
	if got := srv.extraConfig(); got != "" {
		t.Errorf("initial_revealed<=0 应返回空串，实际 %q", got)
	}
	srv.ctl.initialRevealed = 12
	if got, want := srv.extraConfig(), `{"initial_revealed_pieces":12}`; got != want {
		t.Errorf("extraConfig = %q, want %q", got, want)
	}
}
