package scheduler

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"banqi/server/internal/store"
	pb "banqi/server/pb"
)

// fakePresigner 仅记录签发的 URL（不依赖 AWS SDK 与网络）。
type fakePresigner struct {
	mu sync.Mutex
	// puts[object_key] = []url（多次 PUT 重签）
	puts map[string][]string
}

func newFakePresigner() *fakePresigner { return &fakePresigner{puts: map[string][]string{}} }

func (f *fakePresigner) PresignPut(_ context.Context, key string, _ int64) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	u := "https://fake/" + key + "?n=" + intToStr(int64(len(f.puts[key])))
	f.puts[key] = append(f.puts[key], u)
	return u, nil
}

func (f *fakePresigner) PresignGet(_ context.Context, key string) (string, error) {
	return "https://fake-get/" + key, nil
}

func intToStr(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// newEpisodeTestServer 用 fake presigner 构造 Server（不依赖 AWS SDK 与 R2）。
func newEpisodeTestServer(t *testing.T) (*Server, *store.Store, *fakePresigner) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "sched.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	fp := newFakePresigner()
	return &Server{
		cfg:   Config{Variant: "4x8"},
		ctl:   &Control{store: st},
		store: st,
		r2:    fp,
		tasks: map[string]*runningTask{},
	}, st, fp
}

// 验证 1：UNIQUE(task_id) 索引在迁移时被创建（防止 ReportEpisode 重试产生重复行）。
func TestEpisodesTaskIdUniqueIndex(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(filepath.Join(t.TempDir(), "sched.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	if err := s.InsertEpisode(ctx, store.Episode{
		WorkerID: "w", TaskID: "t1", NetworkSha: "sha", GameCount: 1, TotalSteps: 10,
		Winner: 1, ObjectKey: "episodes/sha/a.epb.gz",
	}); err != nil {
		t.Fatalf("insert 1: %v", err)
	}
	if err := s.InsertEpisode(ctx, store.Episode{
		WorkerID: "w", TaskID: "t1", NetworkSha: "sha", GameCount: 1, TotalSteps: 10,
		Winner: 1, ObjectKey: "episodes/sha/b.epb.gz",
	}); err == nil {
		t.Fatal("重复 task_id 应被 UNIQUE 索引拒绝")
	}

	row, err := s.GetEpisodeByTask(ctx, "t1")
	if err != nil || row == nil {
		t.Fatalf("GetEpisodeByTask: %+v err=%v", row, err)
	}
	if row.ObjectKey != "episodes/sha/a.epb.gz" {
		t.Errorf("应返回首次登记的对象键（保留客户端原上传路径），实际 %q", row.ObjectKey)
	}

	if r, _ := s.GetEpisodeByTask(ctx, "missing"); r != nil {
		t.Errorf("缺失 task_id 应返回 nil，实际 %+v", r)
	}
}

// 验证 2：ReportEpisode 同 task_id 重试必须复用首次的对象键 + 重签 PUT URL。
//
// worker 上传失败重试 ReportEpisode 时，旧实现会生成新的 objID + 新元数据行，
// 导致 R2 出现孤儿对象（旧 key 没人引用）+ trainer 拉取到重复 episode。
// 新实现：lookupTask → GetEpisodeByTask → 返回首次的对象键 + 新 PUT URL。
func TestReportEpisodeReusesObjectKey(t *testing.T) {
	ctx := context.Background()
	srv, st, fp := newEpisodeTestServer(t)
	if _, err := st.RegisterNetwork(ctx, "sha-best", "", "best", "onnx"); err != nil {
		t.Fatalf("register best: %v", err)
	}
	if err := st.PromoteBest(ctx, "sha-best"); err != nil {
		t.Fatalf("promote best: %v", err)
	}

	srv.tasks["task-A"] = &runningTask{
		Kind: pb.TaskKind_TASK_SELFPLAY, NetworkSha: "sha-best",
		WorkerID: "w1", Games: 1,
	}

	// 首次上报：生成 objID + InsertEpisode
	req := &pb.EpisodeMeta{
		WorkerId: "w1", TaskId: "task-A", NetworkSha: "sha-best",
		GameCount: 8, TotalSteps: 200, Winner: 1,
		ContentLength: 4096, Kind: pb.DataKind_DATA_RESNET,
	}
	rep1, err := srv.ReportEpisode(ctx, req)
	if err != nil || !rep1.Accepted {
		t.Fatalf("首次上报：ack=%+v err=%v", rep1, err)
	}
	if rep1.ObjectKey == "" || rep1.UploadUrl == "" {
		t.Fatalf("首次上报应带对象键与 URL，实际 %+v", rep1)
	}
	if strings.Contains(rep1.Message, "duplicate") {
		t.Errorf("首次上报 message 不应带 duplicate 标记，实际 %q", rep1.Message)
	}

	// 重试：必须命中幂等闸口，复用对象键
	rep2, err := srv.ReportEpisode(ctx, req)
	if err != nil || !rep2.Accepted {
		t.Fatalf("重试应被接受：ack=%+v err=%v", rep2, err)
	}
	if rep2.ObjectKey != rep1.ObjectKey {
		t.Errorf("重试必须复用首次的对象键：首次=%s 重试=%s", rep1.ObjectKey, rep2.ObjectKey)
	}
	if rep2.UploadUrl == rep1.UploadUrl {
		t.Errorf("重试应签发新的 PUT URL（即便对象键相同，URL 也要新签）")
	}
	if !strings.Contains(rep2.Message, "duplicate") {
		t.Errorf("重试 message 应带 duplicate 标记，实际 %q", rep2.Message)
	}

	// DB 里只有一行（UNIQUE(task_id) 兜底）
	keys, _ := st.ListEpisodeKeys(ctx, "", 10, store.KindAny)
	if len(keys) != 1 || keys[0] != rep1.ObjectKey {
		t.Fatalf("应只有一条 episode 记录，实际 %v", keys)
	}

	// 签发记录：每个对象键被签发两次（首次 + 重试）
	if urls := fp.puts[rep1.ObjectKey]; len(urls) != 2 {
		t.Errorf("对象键应被签发 2 次（首次 + 重试），实际 %d", len(urls))
	}
}

// 验证 3：worker_id 不匹配应优先于幂等闸口被拒（防止恶意 worker 借 Done / 已登记
// 元数据探测他人任务）。
func TestReportEpisodeWorkerMismatchRejected(t *testing.T) {
	ctx := context.Background()
	srv, st, _ := newEpisodeTestServer(t)
	if _, err := st.RegisterNetwork(ctx, "sha-best", "", "best", "onnx"); err != nil {
		t.Fatalf("register best: %v", err)
	}
	if err := st.PromoteBest(ctx, "sha-best"); err != nil {
		t.Fatalf("promote best: %v", err)
	}
	srv.tasks["task-A"] = &runningTask{
		Kind: pb.TaskKind_TASK_SELFPLAY, NetworkSha: "sha-best",
		WorkerID: "w-good", Games: 1,
	}
	// 首次正常上报
	rep1, err := srv.ReportEpisode(ctx, &pb.EpisodeMeta{
		WorkerId: "w-good", TaskId: "task-A", NetworkSha: "sha-best",
		GameCount: 1, Winner: 1, ContentLength: 1024,
	})
	if err != nil || !rep1.Accepted {
		t.Fatalf("首次上报：ack=%+v err=%v", rep1, err)
	}
	// 另一个 worker 持相同 task_id 上报：应被 worker_mismatch 拒
	rep2, err := srv.ReportEpisode(ctx, &pb.EpisodeMeta{
		WorkerId: "w-evil", TaskId: "task-A", NetworkSha: "sha-best",
		GameCount: 1, Winner: 1, ContentLength: 1024,
	})
	if err != nil {
		t.Fatalf("worker_mismatch 不应返回错误: %v", err)
	}
	if rep2.Accepted || !strings.Contains(rep2.Message, "worker_mismatch") {
		t.Errorf("worker_mismatch 应拒绝，实际 ack=%+v", rep2)
	}
}

// 验证 4：未知 task_id 直接拒绝（lookupTask miss 兜底）。
func TestReportEpisodeUnknownTaskIdRejected(t *testing.T) {
	srv, _, _ := newEpisodeTestServer(t)
	resp, err := srv.ReportEpisode(context.Background(), &pb.EpisodeMeta{
		WorkerId: "w1", TaskId: "missing-task", NetworkSha: "x", GameCount: 1,
	})
	if err != nil {
		t.Fatalf("缺任务上报应直接拒绝，不返回错误: %v", err)
	}
	if resp.Accepted || !strings.Contains(resp.Message, "unknown_task_id") {
		t.Errorf("未知 task_id 应拒绝，实际 ack=%+v", resp)
	}
}
