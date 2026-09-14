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
	sched, err := scheduler.New(ctx, scheduler.Config{Variant: "4x8"}, st, presigner)
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

	if _, err := st.RegisterNetwork(ctx, "sha-x", "", ""); err != nil {
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
