package api

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"time"

	"banqi/server/internal/scheduler"
	"banqi/server/internal/sprt"
	"banqi/server/internal/store"
	pb "banqi/server/pb"
)

const (
	defaultEpisodeLimit = 100
	maxEpisodeLimit     = 500
	maxControlBody      = 4096
	// 训练配置覆盖项：可调字段约 30 项，控制请求体的 4KB 上限放不下。
	maxTrainConfigBody = 16384
	// 绝对强度评估：明细与趋势各取多少条（趋势用于画版本曲线，无需全量）
	evalLatestLimit = 200
	evalTrendLimit  = 50
)

type okView struct {
	OK bool `json:"ok"`
}

type networkView struct {
	Sha       string `json:"sha"`
	ParentSha string `json:"parentSha"`
	CreatedAt int64  `json:"createdAt"`
	IsBest    bool   `json:"isBest"`
	Status    string `json:"status"`
	Notes     string `json:"notes"`
}

type sprtView struct {
	Elo0  float64 `json:"elo0"`
	Elo1  float64 `json:"elo1"`
	Alpha float64 `json:"alpha"`
	Beta  float64 `json:"beta"`
}

type statusView struct {
	Variant          string         `json:"variant"`
	Paused           bool           `json:"paused"`
	InitialRevealed  int            `json:"initialRevealed"`
	DataKind         string         `json:"dataKind"`
	MinClientVersion string         `json:"minClientVersion"`
	Sprt             sprtView       `json:"sprt"`
	Best             *networkView   `json:"best"`
	Counts           store.Counts   `json:"counts"`
	Eval             evalConfigView `json:"eval"`
	// ShouldStop 绝对强度判据置位的停机信号（trainer 轮询 GetInfo 后优雅停止）。
	ShouldStop bool   `json:"shouldStop"`
	StopReason string `json:"stopReason"`
}

// evalConfigView 评估配置快照（WebUI 面板表头 + /api/status 展示）。
type evalConfigView struct {
	Enabled          bool     `json:"enabled"`
	Opponents        []string `json:"opponents"`
	Games            int      `json:"games"`
	MctsSims         int      `json:"mctsSims"`
	Mode             string   `json:"mode"`
	EveryNPromotions int      `json:"everyNPromotions"`
	NoProgressN      int      `json:"noProgressN"`
	NoProgressEps    float64  `json:"noProgressEps"`
	Pending          int      `json:"pending"`
}

type evalResultView struct {
	NetworkSha   string  `json:"networkSha"`
	OpponentSpec string  `json:"opponentSpec"`
	Wins         int     `json:"wins"`
	Draws        int     `json:"draws"`
	Losses       int     `json:"losses"`
	NumGames     int     `json:"numGames"`
	WinRate      float64 `json:"winRate"`
	AvgMoves     float64 `json:"avgMoves"`
	CreatedAt    int64   `json:"createdAt"`
}

// evalTrendView 单对手的版本序列（升序），含「连续无提升」计数供界面直接展示。
type evalTrendView struct {
	Opponent      string           `json:"opponent"`
	Versions      int              `json:"versions"`
	NoProgress    int              `json:"noProgress"`
	LatestWinRate float64          `json:"latestWinRate"`
	Points        []evalResultView `json:"points"`
}

type evalView struct {
	Config     evalConfigView   `json:"config"`
	ShouldStop bool             `json:"shouldStop"`
	StopReason string           `json:"stopReason"`
	Latest     []evalResultView `json:"latest"`
	Trends     []evalTrendView  `json:"trends"`
}

// evalModeName 评估搜索模式的可读名（与 scheduler 侧日志口径一致）。
func evalModeName(mctsSims int) string {
	if mctsSims <= 0 {
		return "纯策略 argmax"
	}
	return "MCTS " + strconv.Itoa(mctsSims) + " sims"
}

func toEvalConfigView(rt scheduler.Runtime, pending int) evalConfigView {
	return evalConfigView{
		Enabled:          rt.EvalEnabled,
		Opponents:        rt.EvalOpponents,
		Games:            rt.EvalGames,
		MctsSims:         rt.EvalMctsSims,
		Mode:             evalModeName(rt.EvalMctsSims),
		EveryNPromotions: rt.EvalEveryNPromotions,
		NoProgressN:      rt.EvalNoProgressN,
		NoProgressEps:    rt.EvalNoProgressEps,
		Pending:          pending,
	}
}

func toEvalResultView(e store.EvalResult) evalResultView {
	return evalResultView{
		NetworkSha: e.NetworkSha, OpponentSpec: e.OpponentSpec,
		Wins: e.Wins, Draws: e.Draws, Losses: e.Losses, NumGames: e.NumGames,
		WinRate: e.WinRate(), AvgMoves: e.AvgMoves, CreatedAt: e.CreatedAt.Unix(),
	}
}

type matchView struct {
	ID          int64    `json:"id"`
	Candidate   string   `json:"candidate"`
	Opponent    string   `json:"opponent"`
	Status      string   `json:"status"`
	Pairs       [5]int   `json:"pairs"`
	NumGames    int      `json:"numGames"`
	TargetGames int      `json:"targetGames"`
	Score       *float64 `json:"score"`
	Llr         float64  `json:"llr"`
	Lower       float64  `json:"lower"`
	Upper       float64  `json:"upper"`
	Verdict     string   `json:"verdict"`
}

type workerView struct {
	ID             string `json:"id"`
	LastSeen       int64  `json:"lastSeen"`
	Online         bool   `json:"online"`
	Threads        int    `json:"threads"`
	CompletedGames int    `json:"completedGames"`
	ClientVersion  string `json:"clientVersion"`
	MemoryMb       int64  `json:"memoryMb"`
}

type episodeView struct {
	ID         int64  `json:"id"`
	WorkerID   string `json:"workerId"`
	TaskID     string `json:"taskId"`
	NetworkSha string `json:"networkSha"`
	GameCount  int    `json:"gameCount"`
	TotalSteps int    `json:"totalSteps"`
	Winner     int    `json:"winner"`
	DataKind   string `json:"dataKind"`
	ObjectKey  string `json:"objectKey"`
	CreatedAt  int64  `json:"createdAt"`
}

type episodePageView struct {
	Items      []episodeView `json:"items"`
	NextBefore int64         `json:"nextBefore"`
	HasMore    bool          `json:"hasMore"`
}

type taskView struct {
	TaskID      string `json:"taskId"`
	WorkerID    string `json:"workerId"`
	Kind        string `json:"kind"`
	MatchID     int64  `json:"matchId"`
	NetworkSha  string `json:"networkSha"`
	OpponentSha string `json:"opponentSha"`
	// OpponentSpec eval 任务的对手标识（rule:capture_first 等）；其余任务为空。
	OpponentSpec string `json:"opponentSpec"`
	Games        int    `json:"games"`
	CreatedAt    int64  `json:"createdAt"`
}

type controlRequest struct {
	Paused          *bool   `json:"paused"`
	InitialRevealed *int    `json:"initialRevealed"`
	DataKind        *string `json:"dataKind"` // resnet / nnue
	// ClearStop 清除绝对强度判据置位的停机信号（确认续训时使用）。
	ClearStop *bool `json:"clearStop"`
}

func toNetworkView(n store.Network) networkView {
	return networkView{
		Sha: n.Sha, ParentSha: n.ParentSha, CreatedAt: n.CreatedAt.Unix(),
		IsBest: n.IsBest, Status: n.Status, Notes: n.Notes,
	}
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	best, err := s.store.GetBest(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	counts, err := s.store.Counts(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	rt := s.sched.Runtime()
	view := statusView{
		Variant:          rt.Variant,
		Paused:           rt.Paused,
		InitialRevealed:  rt.InitialRevealed,
		DataKind:         scheduler.DataKindName(rt.DataKind),
		MinClientVersion: rt.MinClientVersion,
		Sprt:             sprtView{Elo0: rt.SprtElo0, Elo1: rt.SprtElo1, Alpha: rt.SprtAlpha, Beta: rt.SprtBeta},
		Counts:           counts,
		Eval:             toEvalConfigView(rt, s.sched.PendingEval()),
		ShouldStop:       s.sched.Control().ShouldStop(),
		StopReason:       s.sched.Control().StopReason(),
	}
	if best != nil {
		nv := toNetworkView(*best)
		view.Best = &nv
	}
	writeJSON(w, http.StatusOK, view)
}

func (s *Server) handleNetworks(w http.ResponseWriter, r *http.Request) {
	items, err := s.store.ListNetworks(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := make([]networkView, 0, len(items))
	for _, n := range items {
		out = append(out, toNetworkView(n))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handlePromote(w http.ResponseWriter, r *http.Request) {
	sha := r.PathValue("sha")
	n, err := s.store.GetNetwork(r.Context(), sha)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if n == nil {
		writeError(w, http.StatusNotFound, "network not found: "+sha)
		return
	}
	if n.IsBest {
		writeError(w, http.StatusConflict, "already best: "+sha)
		return
	}
	if err := s.store.PromoteBest(r.Context(), sha); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	log.Printf("[webui] manual promote sha=%s status=%s", sha, n.Status)
	writeJSON(w, http.StatusOK, okView{OK: true})
}

func (s *Server) handleMatches(w http.ResponseWriter, r *http.Request) {
	items, err := s.store.ListMatches(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	rt := s.sched.Runtime()
	bounds := sprt.NewBounds(rt.SprtAlpha, rt.SprtBeta)
	out := make([]matchView, 0, len(items))
	for _, m := range items {
		mv := matchView{
			ID: m.ID, Candidate: m.Candidate, Opponent: m.Opponent, Status: m.Status,
			Pairs: m.Pairs, NumGames: m.NumGames, TargetGames: m.TargetGames,
			Lower: bounds.Lower, Upper: bounds.Upper, Verdict: sprt.Continue.String(),
		}
		p := sprt.Pentanomial(m.Pairs)
		if p.Total() > 0 {
			llr, err := sprt.LLR(p, rt.SprtElo0, rt.SprtElo1)
			if err != nil {
				log.Printf("[webui] llr match=%d: %v", m.ID, err)
			} else {
				mv.Llr = llr
				if score, _, err := p.Stats(); err == nil {
					mv.Score = &score
				}
			}
			if verdict, _, err := sprt.Judge(p, rt.SprtElo0, rt.SprtElo1, bounds); err == nil {
				mv.Verdict = verdict.String()
			}
		}
		out = append(out, mv)
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleWorkers(w http.ResponseWriter, r *http.Request) {
	items, err := s.store.ListWorkers(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	now := time.Now()
	out := make([]workerView, 0, len(items))
	for _, wk := range items {
		out = append(out, workerView{
			ID: wk.ID, LastSeen: wk.LastSeen.Unix(),
			Online:         now.Sub(wk.LastSeen) < s.onlineWindow,
			Threads:        wk.Threads,
			CompletedGames: wk.CompletedGames,
			ClientVersion:  wk.ClientVersion,
			MemoryMb:       wk.MemoryMb,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleEpisodes(w http.ResponseWriter, r *http.Request) {
	before, _ := strconv.ParseInt(r.URL.Query().Get("before"), 10, 64)
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > maxEpisodeLimit {
		limit = defaultEpisodeLimit
	}
	items, err := s.store.ListEpisodes(r.Context(), before, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := make([]episodeView, 0, len(items))
	for _, e := range items {
		out = append(out, episodeView{
			ID: e.ID, WorkerID: e.WorkerID, TaskID: e.TaskID, NetworkSha: e.NetworkSha,
			GameCount: e.GameCount, TotalSteps: e.TotalSteps, Winner: e.Winner,
			DataKind:  scheduler.DataKindName(pb.DataKind(e.Kind)),
			ObjectKey: e.ObjectKey, CreatedAt: e.CreatedAt.Unix(),
		})
	}
	next := before
	if len(items) > 0 {
		next = items[len(items)-1].ID
	}
	writeJSON(w, http.StatusOK, episodePageView{Items: out, NextBefore: next, HasMore: len(items) == limit})
}

func (s *Server) handleTasks(w http.ResponseWriter, r *http.Request) {
	tasks := s.sched.RunningTasks()
	out := make([]taskView, 0, len(tasks))
	for _, t := range tasks {
		out = append(out, taskView{
			TaskID: t.TaskID, WorkerID: t.WorkerID, Kind: t.Kind.String(), MatchID: t.MatchID,
			NetworkSha: t.NetworkSha, OpponentSha: t.OpponentSha, OpponentSpec: t.OpponentSpec,
			Games: t.Games, CreatedAt: t.CreatedAt.Unix(),
		})
	}
	writeJSON(w, http.StatusOK, out)
}

// handleEval 返回绝对强度评估的趋势视图：按对手分组的版本序列（升序）+ 最新明细 +
// 「连续无提升」计数 + 评估配置。界面据此画曲线并显示停机信号。
func (s *Server) handleEval(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	rt := s.sched.Runtime()

	items, err := s.store.ListEvalResults(ctx, evalLatestLimit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	latest := make([]evalResultView, 0, len(items))
	for _, e := range items {
		latest = append(latest, toEvalResultView(e))
	}

	opponents, err := s.store.ListEvalOpponents(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	trends := make([]evalTrendView, 0, len(opponents))
	for _, opp := range opponents {
		points, err := s.store.EvalTrend(ctx, opp, evalTrendLimit)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		tv := evalTrendView{Opponent: opp, Points: make([]evalResultView, 0, len(points))}
		for _, p := range points {
			tv.Points = append(tv.Points, toEvalResultView(p))
		}
		if prog, ok := s.sched.EvalProgressFor(ctx, opp, 0); ok {
			tv.Versions = prog.Versions
			tv.NoProgress = prog.NoProgress
			tv.LatestWinRate = prog.LatestWinRate
		}
		trends = append(trends, tv)
	}

	writeJSON(w, http.StatusOK, evalView{
		Config:     toEvalConfigView(rt, s.sched.PendingEval()),
		ShouldStop: s.sched.Control().ShouldStop(),
		StopReason: s.sched.Control().StopReason(),
		Latest:     latest,
		Trends:     trends,
	})
}

func (s *Server) handleControl(w http.ResponseWriter, r *http.Request) {
	var req controlRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxControlBody)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body: "+err.Error())
		return
	}
	if req.Paused != nil {
		if err := s.sched.Control().SetPaused(r.Context(), *req.Paused); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		log.Printf("[webui] pause_self_play=%v", *req.Paused)
	}
	if req.InitialRevealed != nil {
		if err := s.sched.Control().SetInitialRevealed(r.Context(), *req.InitialRevealed); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		log.Printf("[webui] initial_revealed=%d", *req.InitialRevealed)
	}
	if req.DataKind != nil {
		kind, err := scheduler.ParseDataKind(*req.DataKind)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if err := s.sched.Control().SetDataKind(r.Context(), kind); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		log.Printf("[webui] data_kind=%s", scheduler.DataKindName(kind))
	}
	if req.ClearStop != nil && *req.ClearStop {
		if err := s.sched.Control().ClearStop(r.Context()); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		log.Printf("[webui] eval should_stop 已清除（确认续训）")
	}
	writeJSON(w, http.StatusOK, okView{OK: true})
}

// trainConfigView 训练配置面板视图：当前覆盖项 + 可调字段清单。字段清单来自调度器
// 的白名单，前端因此不必硬编码字段名，新增可调项无需改前端。
type trainConfigView struct {
	Overrides map[string]string            `json:"overrides"`
	Fields    []scheduler.TrainConfigField `json:"fields"`
}

// trainConfigRequest 是 PUT /api/train-config 的请求体：**全量替换**语义，
// 未出现的字段即删除覆盖、回落 trainer 本地配置（空对象 = 清空全部覆盖）。
type trainConfigRequest struct {
	Overrides map[string]string `json:"overrides"`
}

func (s *Server) handleTrainConfigGet(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, trainConfigView{
		Overrides: s.sched.Control().TrainConfig(),
		Fields:    scheduler.TrainConfigFields(),
	})
}

// handleTrainConfigPut 全量替换训练配置覆盖项；字段名与取值由白名单校验，非法输入
// 直接 400 回显原因（不静默丢弃，也不落库半套配置）。
func (s *Server) handleTrainConfigPut(w http.ResponseWriter, r *http.Request) {
	var req trainConfigRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxTrainConfigBody)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body: "+err.Error())
		return
	}
	if err := s.sched.Control().SetTrainConfig(r.Context(), req.Overrides); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	log.Printf("[webui] train_config 已更新 %d 项: %v", len(req.Overrides), req.Overrides)
	writeJSON(w, http.StatusOK, okView{OK: true})
}
