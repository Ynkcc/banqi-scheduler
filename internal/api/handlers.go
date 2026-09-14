package api

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"time"

	"banqi/server/internal/sprt"
	"banqi/server/internal/store"
)

const (
	defaultEpisodeLimit = 100
	maxEpisodeLimit     = 500
	maxControlBody      = 4096
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
	Variant          string       `json:"variant"`
	Paused           bool         `json:"paused"`
	InitialRevealed  int          `json:"initialRevealed"`
	MinClientVersion string       `json:"minClientVersion"`
	Sprt             sprtView     `json:"sprt"`
	Best             *networkView `json:"best"`
	Counts           store.Counts `json:"counts"`
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
	Games       int    `json:"games"`
	CreatedAt   int64  `json:"createdAt"`
}

type controlRequest struct {
	Paused          *bool `json:"paused"`
	InitialRevealed *int  `json:"initialRevealed"`
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
		MinClientVersion: rt.MinClientVersion,
		Sprt:             sprtView{Elo0: rt.SprtElo0, Elo1: rt.SprtElo1, Alpha: rt.SprtAlpha, Beta: rt.SprtBeta},
		Counts:           counts,
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
			NetworkSha: t.NetworkSha, OpponentSha: t.OpponentSha, Games: t.Games, CreatedAt: t.CreatedAt.Unix(),
		})
	}
	writeJSON(w, http.StatusOK, out)
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
	writeJSON(w, http.StatusOK, okView{OK: true})
}
