package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"banqi/server/internal/r2"
	"banqi/server/internal/scheduler"
	"banqi/server/internal/store"
)

func newTestServer(t *testing.T) (*Server, *store.Store) {
	t.Helper()
	return newTestServerWithConfig(t, scheduler.Config{Variant: "4x8"})
}

func newTestServerWithConfig(t *testing.T, cfg scheduler.Config) (*Server, *store.Store) {
	t.Helper()
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "scheduler.db"))
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	presigner, err := r2.New(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("r2: %v", err)
	}
	sched, err := scheduler.New(ctx, cfg, st, presigner)
	if err != nil {
		t.Fatalf("scheduler: %v", err)
	}
	return New(st, sched, time.Minute), st
}

func do(t *testing.T, s *Server, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(method, path, nil)
	} else {
		r = httptest.NewRequest(method, path, strings.NewReader(body))
	}
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, r)
	return rec
}

func TestStatusAndTasksEndpoints(t *testing.T) {
	s, _ := newTestServer(t)

	rec := do(t, s, http.MethodGet, "/api/status", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var status map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &status); err != nil {
		t.Fatalf("json: %v", err)
	}
	if status["variant"] != "4x8" {
		t.Errorf("variant = %v, want 4x8", status["variant"])
	}
	if status["paused"] != false {
		t.Errorf("paused = %v, want false", status["paused"])
	}

	if rec := do(t, s, http.MethodGet, "/api/tasks", ""); rec.Code != http.StatusOK {
		t.Errorf("tasks status=%d", rec.Code)
	}
	if rec := do(t, s, http.MethodGet, "/api/unknown", ""); rec.Code != http.StatusNotFound {
		t.Errorf("未知 /api 路径应为 404, 实际 %d", rec.Code)
	}
}

func TestPromoteEndpoint(t *testing.T) {
	s, st := newTestServer(t)
	ctx := context.Background()

	if _, err := st.RegisterNetwork(ctx, "sha-x", "", "", "onnx"); err != nil {
		t.Fatalf("register: %v", err)
	}

	rec := do(t, s, http.MethodPost, "/api/networks/sha-x/promote", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("promote status=%d body=%s", rec.Code, rec.Body.String())
	}
	best, err := st.GetBest(ctx)
	if err != nil || best == nil || best.Sha != "sha-x" {
		t.Fatalf("best = %+v err=%v", best, err)
	}

	// 已是 best：冲突
	if rec := do(t, s, http.MethodPost, "/api/networks/sha-x/promote", ""); rec.Code != http.StatusConflict {
		t.Errorf("重复晋级应为 409, 实际 %d", rec.Code)
	}
	// 不存在的 sha
	if rec := do(t, s, http.MethodPost, "/api/networks/missing/promote", ""); rec.Code != http.StatusNotFound {
		t.Errorf("未知 sha 应为 404, 实际 %d", rec.Code)
	}
}

func TestControlEndpoint(t *testing.T) {
	s, _ := newTestServer(t)

	if rec := do(t, s, http.MethodPost, "/api/control", `{"paused":true}`); rec.Code != http.StatusOK {
		t.Fatalf("control status=%d body=%s", rec.Code, rec.Body.String())
	}
	var status map[string]any
	if err := json.Unmarshal(do(t, s, http.MethodGet, "/api/status", "").Body.Bytes(), &status); err != nil {
		t.Fatalf("json: %v", err)
	}
	if status["paused"] != true {
		t.Errorf("暂停后 paused = %v, want true", status["paused"])
	}

	// 课程阶段：合法值生效
	if rec := do(t, s, http.MethodPost, "/api/control", `{"initialRevealed":12}`); rec.Code != http.StatusOK {
		t.Fatalf("initialRevealed status=%d", rec.Code)
	}
	if err := json.Unmarshal(do(t, s, http.MethodGet, "/api/status", "").Body.Bytes(), &status); err != nil {
		t.Fatalf("json: %v", err)
	}
	if status["initialRevealed"] != float64(12) {
		t.Errorf("initialRevealed = %v, want 12", status["initialRevealed"])
	}

	// 非法负值 → 400
	if rec := do(t, s, http.MethodPost, "/api/control", `{"initialRevealed":-1}`); rec.Code != http.StatusBadRequest {
		t.Errorf("负 initialRevealed 应为 400, 实际 %d", rec.Code)
	}
	// 非法 JSON → 400
	if rec := do(t, s, http.MethodPost, "/api/control", `{`); rec.Code != http.StatusBadRequest {
		t.Errorf("非法 JSON 应为 400, 实际 %d", rec.Code)
	}
}

// 绝对强度面板的数据契约：配置、按对手分组的升序趋势、无提升计数、停机信号与清除。
func TestEvalEndpointAndStopFlag(t *testing.T) {
	ctx := context.Background()
	s, st := newTestServerWithConfig(t, scheduler.Config{
		Variant:           "4x8",
		EvalEnabled:       true,
		EvalOpponents:     []string{"random", "rule:capture_first"},
		EvalGames:         1000,
		EvalNoProgressN:   2,
		EvalNoProgressEps: 0.02,
	})

	// 造三代停滞数据（60.0% → 60.5% → 60.8%，两次提升均 < 2pt）
	for i, w := range []int{600, 605, 608} {
		if err := st.UpsertEvalResult(ctx, store.EvalResult{
			NetworkSha:   []string{"v1", "v2", "v3"}[i],
			OpponentSpec: "rule:capture_first",
			Wins:         w, Losses: 1000 - w, NumGames: 1000, AvgMoves: 44.4,
			CreatedAt: time.Unix(int64(i+1), 0),
		}); err != nil {
			t.Fatalf("seed eval: %v", err)
		}
	}

	rec := do(t, s, http.MethodGet, "/api/eval", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("eval status=%d body=%s", rec.Code, rec.Body.String())
	}
	var view map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &view); err != nil {
		t.Fatalf("json: %v", err)
	}
	cfg, _ := view["config"].(map[string]any)
	if cfg == nil || cfg["enabled"] != true || cfg["mode"] != "纯策略 argmax" {
		t.Errorf("config 视图不符: %+v", cfg)
	}
	latest, _ := view["latest"].([]any)
	if len(latest) != 3 {
		t.Errorf("latest 应有 3 条，实际 %d", len(latest))
	}
	trends, _ := view["trends"].([]any)
	if len(trends) != 1 {
		t.Fatalf("trends 应只含已评测的 1 个对手，实际 %d", len(trends))
	}
	tr, _ := trends[0].(map[string]any)
	if tr["opponent"] != "rule:capture_first" {
		t.Errorf("trend opponent = %v", tr["opponent"])
	}
	if tr["noProgress"] != float64(2) {
		t.Errorf("noProgress = %v, want 2（两次提升均 < 2pt）", tr["noProgress"])
	}
	points, _ := tr["points"].([]any)
	if len(points) != 3 {
		t.Fatalf("趋势点应有 3 个，实际 %d", len(points))
	}
	first, _ := points[0].(map[string]any)
	if first["networkSha"] != "v1" {
		t.Errorf("趋势点应升序（老→新），首点 = %v", first["networkSha"])
	}
	if wr, _ := first["winRate"].(float64); wr < 0.5999 || wr > 0.6001 {
		t.Errorf("首点胜率 = %v, want 0.6", wr)
	}

	// 未置位时不显示停机
	if view["shouldStop"] != false {
		t.Errorf("初始 shouldStop 应为 false，实际 %v", view["shouldStop"])
	}

	// 置位 → /api/status 与 /api/eval 都应暴露
	if err := s.sched.Control().SetStop(ctx, "eval_no_progress: 测试"); err != nil {
		t.Fatalf("SetStop: %v", err)
	}
	var status map[string]any
	if err := json.Unmarshal(do(t, s, http.MethodGet, "/api/status", "").Body.Bytes(), &status); err != nil {
		t.Fatalf("status json: %v", err)
	}
	if status["shouldStop"] != true || status["stopReason"] != "eval_no_progress: 测试" {
		t.Errorf("status 未暴露停机信号: shouldStop=%v reason=%v", status["shouldStop"], status["stopReason"])
	}

	// 通过 control 清除（确认续训）
	if rec := do(t, s, http.MethodPost, "/api/control", `{"clearStop":true}`); rec.Code != http.StatusOK {
		t.Fatalf("clearStop status=%d body=%s", rec.Code, rec.Body.String())
	}
	if err := json.Unmarshal(do(t, s, http.MethodGet, "/api/status", "").Body.Bytes(), &status); err != nil {
		t.Fatalf("status json: %v", err)
	}
	if status["shouldStop"] != false || status["stopReason"] != "" {
		t.Errorf("清除后应无停机信号，实际 shouldStop=%v reason=%v", status["shouldStop"], status["stopReason"])
	}
}
