package scheduler

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"sync"
	"time"

	"banqi/server/internal/r2"
	"banqi/server/internal/sprt"
	"banqi/server/internal/store"
	pb "banqi/server/pb"
)

type Config struct {
	GamesPerTask      int     // selfplay 每次下发的局数
	GatekeeperGames   int     // gatekeeper 对打目标局数（成对）
	SprtElo0          float64
	SprtElo1          float64
	SprtAlpha         float64
	SprtBeta          float64
	MinClientVersion  string
}

type runningTask struct {
	Kind        pb.TaskKind
	MatchID     int64
	NetworkSha  string
	OpponentSha string
	Games       int
	WorkerID    string
	CreatedAt   time.Time
}

type Server struct {
	pb.UnimplementedSchedulerServiceServer
	cfg   Config
	store *store.Store
	r2    *r2.Presigner

	mu    sync.Mutex
	tasks map[string]*runningTask
}

func New(cfg Config, st *store.Store, presigner *r2.Presigner) *Server {
	return &Server{cfg: cfg, store: st, r2: presigner, tasks: make(map[string]*runningTask)}
}

func newID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Errorf("generate random id: %w", err))
	}
	return hex.EncodeToString(b)
}

func (s *Server) GetTask(ctx context.Context, req *pb.TaskRequest) (*pb.TaskResponse, error) {
	if s.cfg.MinClientVersion != "" && req.ClientVersion != "" && req.ClientVersion < s.cfg.MinClientVersion {
		return &pb.TaskResponse{Kind: pb.TaskKind_TASK_NONE, Message: "client_version_too_old"}, nil
	}

	// 优先下发未完结的 gatekeeper 对打（lczero target_slice 模式）
	m, err := s.store.PendingMatch()
	if err != nil {
		return nil, err
	}
	if m != nil {
		taskID := newID()
		resp, err := s.ratingTask(ctx, taskID, m, req)
		if err != nil {
			return nil, err
		}
		s.mu.Lock()
		s.tasks[taskID] = &runningTask{Kind: pb.TaskKind_TASK_RATING, MatchID: m.ID,
			NetworkSha: m.Candidate, OpponentSha: m.Opponent, Games: int(resp.Games), WorkerID: req.WorkerId, CreatedAt: time.Now()}
		s.mu.Unlock()
		log.Printf("[task] rating assigned worker=%s match=%d candidate=%s opponent=%s", req.WorkerId, m.ID, m.Candidate, m.Opponent)
		return resp, nil
	}

	// 常规 selfplay：拉 best 网络
	best, err := s.store.GetBest()
	if err != nil {
		return nil, err
	}
	if best == nil {
		return &pb.TaskResponse{Kind: pb.TaskKind_TASK_NONE, Message: "no_best_network_registered"}, nil
	}
	taskID := newID()
	resp := &pb.TaskResponse{
		TaskId:            taskID,
		Kind:              pb.TaskKind_TASK_SELFPLAY,
		NetworkSha:        best.Sha,
		NetworkShaRemote:  best.Sha,
		Games:             int32(s.cfg.GamesPerTask),
		Params:            &pb.SelfPlayParams{Variant: "4x8"},
	}
	if req.CurrentNetwork != best.Sha {
		url, err := s.r2.PresignGet(ctx, r2.NetworkKey(best.Sha))
		if err != nil {
			return nil, err
		}
		resp.NetworkUrl = url
	}
	s.mu.Lock()
	s.tasks[taskID] = &runningTask{Kind: pb.TaskKind_TASK_SELFPLAY, NetworkSha: best.Sha, Games: int(resp.Games), WorkerID: req.WorkerId, CreatedAt: time.Now()}
	s.mu.Unlock()
	log.Printf("[task] selfplay assigned worker=%s task=%s network=%s games=%d", req.WorkerId, taskID, best.Sha, resp.Games)
	return resp, nil
}

func (s *Server) ratingTask(ctx context.Context, taskID string, m *store.Match, req *pb.TaskRequest) (*pb.TaskResponse, error) {
	remaining := m.TargetGames*2 - m.NumGames
	if remaining <= 0 {
		remaining = 2
	}
	games := remaining
	if games > s.cfg.GamesPerTask {
		games = s.cfg.GamesPerTask
	}
	resp := &pb.TaskResponse{
		TaskId:           taskID,
		Kind:             pb.TaskKind_TASK_RATING,
		NetworkSha:       m.Candidate,
		OpponentSha:      m.Opponent,
		NetworkShaRemote: m.Candidate,
		Games:            int32(games),
		Params:           &pb.SelfPlayParams{Variant: "4x8"},
	}
	candidateURL, err := s.r2.PresignGet(ctx, r2.NetworkKey(m.Candidate))
	if err != nil {
		return nil, err
	}
	opponentURL, err := s.r2.PresignGet(ctx, r2.NetworkKey(m.Opponent))
	if err != nil {
		return nil, err
	}
	resp.NetworkUrl = candidateURL
	resp.OpponentUrl = opponentURL
	return resp, nil
}

func (s *Server) ReportEpisode(ctx context.Context, req *pb.EpisodeMeta) (*pb.EpisodeAck, error) {
	s.mu.Lock()
	task, ok := s.tasks[req.TaskId]
	s.mu.Unlock()
	if !ok {
		return &pb.EpisodeAck{Accepted: false, Message: "unknown_task_id:" + req.TaskId}, nil
	}
	if task.NetworkSha != req.NetworkSha {
		return &pb.EpisodeAck{Accepted: false, Message: fmt.Sprintf("network_sha_mismatch task=%s got=%s", task.NetworkSha, req.NetworkSha)}, nil
	}
	key := r2.EpisodeKey(req.NetworkSha, newID())
	url, err := s.r2.PresignPut(ctx, key, req.ContentSha256, req.ContentLength)
	if err != nil {
		return nil, err
	}
	if err := s.store.InsertEpisode(store.Episode{
		WorkerID: req.WorkerId, TaskID: req.TaskId, NetworkSha: req.NetworkSha,
		GameCount: int(req.GameCount), TotalSteps: int(req.TotalSteps), Winner: int(req.Winner), ObjectKey: key,
	}); err != nil {
		return nil, err
	}
	log.Printf("[episode] worker=%s task=%s network=%s games=%d steps=%d -> %s",
		req.WorkerId, req.TaskId, req.NetworkSha, req.GameCount, req.TotalSteps, key)
	return &pb.EpisodeAck{Accepted: true, UploadUrl: url, ObjectKey: key}, nil
}

func (s *Server) GetNetwork(ctx context.Context, req *pb.NetworkRequest) (*pb.NetworkInfo, error) {
	var n *store.Network
	var err error
	if req.Sha == "" {
		n, err = s.store.GetBest()
	} else {
		n, err = s.store.GetNetwork(req.Sha)
	}
	if err != nil {
		return nil, err
	}
	if n == nil {
		return nil, fmt.Errorf("network not found sha=%q", req.Sha)
	}
	url, err := s.r2.PresignGet(ctx, r2.NetworkKey(n.Sha))
	if err != nil {
		return nil, err
	}
	return &pb.NetworkInfo{
		Sha: n.Sha, DownloadUrl: url, CreatedAt: n.CreatedAt.Unix(), IsBest: n.IsBest,
		GameData: fmt.Sprintf(`{"parent_sha":%q}`, n.ParentSha),
	}, nil
}

func (s *Server) RegisterNetwork(ctx context.Context, req *pb.RegisterNetworkRequest) (*pb.RegisterNetworkAck, error) {
	best, err := s.store.GetBest()
	if err != nil {
		return nil, err
	}
	if best != nil && best.Sha == req.Sha {
		return &pb.RegisterNetworkAck{Accepted: false, Message: "sha_equals_current_best"}, nil
	}
	created, err := s.store.RegisterNetwork(req.Sha, req.ParentSha, req.Notes)
	if err != nil {
		return nil, err
	}
	if !created {
		return &pb.RegisterNetworkAck{Accepted: false, Message: "duplicate_sha:" + req.Sha}, nil
	}
	if best == nil {
		// 首个网络直接晋级
		if err := s.store.PromoteBest(req.Sha); err != nil {
			return nil, err
		}
		log.Printf("[network] first network promoted sha=%s", req.Sha)
		return &pb.RegisterNetworkAck{Accepted: true, Message: "first_network_promoted"}, nil
	}
	matchID, err := s.store.CreateMatch(req.Sha, best.Sha, s.cfg.GatekeeperGames)
	if err != nil {
		return nil, err
	}
	log.Printf("[gatekeeper] match=%d created candidate=%s vs best=%s target_pairs=%d",
		matchID, req.Sha, best.Sha, s.cfg.GatekeeperGames)
	return &pb.RegisterNetworkAck{Accepted: true, MatchTaskHint: fmt.Sprintf("gatekeeper match %d vs %s", matchID, best.Sha)}, nil
}

func (s *Server) ReportMatchResult(ctx context.Context, req *pb.MatchResult) (*pb.MatchResultAck, error) {
	s.mu.Lock()
	task, ok := s.tasks[req.TaskId]
	s.mu.Unlock()
	if !ok {
		return &pb.MatchResultAck{Accepted: false, Message: "unknown_task_id:" + req.TaskId}, nil
	}
	if task.Kind != pb.TaskKind_TASK_RATING {
		return &pb.MatchResultAck{Accepted: false, Message: "task_is_not_rating"}, nil
	}
	m, err := s.store.GetMatch(task.MatchID)
	if err != nil {
		return nil, err
	}
	if m == nil || m.Status != "running" {
		return &pb.MatchResultAck{Accepted: false, Message: fmt.Sprintf("match_%d_not_running", task.MatchID)}, nil
	}

	pairs := m.Pairs
	pairs[0] += int(req.PairLl)
	pairs[1] += int(req.PairLd)
	pairs[2] += int(req.PairDd)
	pairs[3] += int(req.PairDw)
	pairs[4] += int(req.PairWw)
	numGames := m.NumGames + int(req.Games)

	verdict := sprt.Continue
	var llr float64
	if numGames >= m.TargetGames*2 || sprt.Pentanomial(pairs).Total() >= m.TargetGames {
		p := sprt.Pentanomial(pairs)
		bounds := sprt.NewBounds(s.cfg.SprtAlpha, s.cfg.SprtBeta)
		verdict, llr, err = sprt.Judge(p, s.cfg.SprtElo0, s.cfg.SprtElo1, bounds)
		if err != nil {
			return nil, err
		}
		if verdict == sprt.Continue && numGames >= m.TargetGames*2 {
			// 打满目标局数仍无结论：按 LLR 符号判定，LLR>0 视为达标
			if llr > 0 {
				verdict = sprt.AcceptH1
			} else {
				verdict = sprt.RejectH0
			}
			log.Printf("[gatekeeper] match=%d exhausted target_games=%d llr=%.3f forced verdict=%s", m.ID, m.TargetGames, llr, verdict)
		}
	}

	status := "running"
	promoted := false
	best, err := s.store.GetBest()
	if err != nil {
		return nil, err
	}
	bestSha := ""
	if best != nil {
		bestSha = best.Sha
	}
	if verdict == sprt.AcceptH1 {
		status = "concluded"
		if err := s.store.PromoteBest(m.Candidate); err != nil {
			return nil, err
		}
		promoted = true
		bestSha = m.Candidate
	} else if verdict == sprt.RejectH0 {
		status = "concluded"
		if err := s.store.UpdateNetworkStatus(m.Candidate, "rejected"); err != nil {
			return nil, err
		}
	}
	if err := s.store.UpdateMatchResult(m.ID, pairs, numGames, status); err != nil {
		return nil, err
	}
	if verdict != sprt.Continue {
		log.Printf("[gatekeeper] match=%d concluded verdict=%s llr=%.3f pairs=%v promoted=%v", m.ID, verdict, llr, pairs, promoted)
	}
	return &pb.MatchResultAck{Accepted: true, MatchConcluded: verdict != sprt.Continue, Promoted: promoted, BestSha: bestSha}, nil
}

func (s *Server) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatReply, error) {
	if err := s.store.TouchWorker(req.WorkerId, int(req.CurrentThreads), int(req.CompletedGames)); err != nil {
		return nil, err
	}
	best, err := s.store.GetBest()
	if err != nil {
		return nil, err
	}
	reply := &pb.HeartbeatReply{}
	if best != nil {
		reply.BestNetwork = best.Sha
	}
	return reply, nil
}
