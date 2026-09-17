package api

import (
	"embed"
	"encoding/json"
	"log"
	"mime"
	"net/http"
	"path"
	"strings"
	"time"

	"banqi/server/internal/scheduler"
	"banqi/server/internal/store"
)

//go:embed all:dist
var distFS embed.FS

type Server struct {
	store        *store.Store
	sched        *scheduler.Server
	onlineWindow time.Duration
	handler      http.Handler
}

// New 组装 WebUI 的 JSON API 与静态资源路由。
// onlineWindow 为 worker 在线判定窗口。
func New(st *store.Store, sched *scheduler.Server, onlineWindow time.Duration) *Server {
	s := &Server{store: st, sched: sched, onlineWindow: onlineWindow}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/status", s.handleStatus)
	mux.HandleFunc("GET /api/networks", s.handleNetworks)
	mux.HandleFunc("POST /api/networks/{sha}/promote", s.handlePromote)
	mux.HandleFunc("GET /api/matches", s.handleMatches)
	mux.HandleFunc("GET /api/eval", s.handleEval)
	mux.HandleFunc("GET /api/workers", s.handleWorkers)
	mux.HandleFunc("GET /api/episodes", s.handleEpisodes)
	mux.HandleFunc("GET /api/tasks", s.handleTasks)
	mux.HandleFunc("POST /api/control", s.handleControl)
	mux.HandleFunc("/", s.handleStatic)
	s.handler = mux
	return s
}

func (s *Server) Handler() http.Handler { return s.handler }

// HTTPServer 返回绑定本 Server 路由的 http.Server；由调用方负责 ListenAndServe 与 Shutdown，
// 避免在包内持有生命周期状态（进程退出时的优雅关闭由 cmd/scheduler 统一编排）。
func (s *Server) HTTPServer(addr string) *http.Server {
	return &http.Server{Addr: addr, Handler: s.handler, ReadHeaderTimeout: 5 * time.Second}
}

// handleStatic 从 embed 产物里取文件；未命中的非 /api 路径回落到 index.html（前端路由）。
func (s *Server) handleStatic(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if strings.HasPrefix(r.URL.Path, "/api/") {
		writeError(w, http.StatusNotFound, "unknown api path: "+r.URL.Path)
		return
	}
	name := strings.TrimPrefix(path.Clean(r.URL.Path), "/")
	if name == "" || name == "." {
		name = "index.html"
	}
	if data, err := distFS.ReadFile("dist/" + name); err == nil {
		w.Header().Set("Content-Type", contentType(name))
		if _, err := w.Write(data); err != nil {
			log.Printf("[webui] write %s: %v", name, err)
		}
		return
	}
	index, err := distFS.ReadFile("dist/index.html")
	if err != nil {
		http.Error(w, "webui 未构建：在 webui/ 执行 npm install && npm run build 后重新编译调度器", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if _, err := w.Write(index); err != nil {
		log.Printf("[webui] write index.html: %v", err)
	}
}

func contentType(name string) string {
	if ct := mime.TypeByExtension(path.Ext(name)); ct != "" {
		return ct
	}
	return "application/octet-stream"
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("[webui] encode json: %v", err)
	}
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
