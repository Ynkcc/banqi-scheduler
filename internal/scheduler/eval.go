package scheduler

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"banqi/server/internal/r2"
	"banqi/server/internal/store"
	pb "banqi/server/pb"
)

// 绝对强度评估（eval）编排。
//
// 把「评估」建模为一种新的调度任务（TASK_EVAL），完全复用既有的
// 「下发任务 → worker 执行 → 上报结果 → 服务端聚合」主链路：执行能力在采集端
// （Rust 棋引擎 + 规则策略），调度器只负责编排与判定。
//
// 与 gatekeeper（rating）的分工：
//   - rating 是**相对**门禁：candidate vs 当前 best + GSPRT，只回答「谁当 best」；
//   - eval 是**绝对**强度观测：best vs 规则/内建对手，落 eval_results、不参与晋级，
//     回答「这一代能不能赢一个 3 行的优先吃子启发式」。
//
// 后者恰是相对门禁在结构上无法发现的问题：历史上 380 个版本一路晋级（gatekeeper
// 只比较相邻两代），而同一模型的纯策略对优先吃子只有 57.6%。
//
// 触发：best 指针变更时（首个网络晋级 / gatekeeper 晋级）按 EvalEveryNPromotions 节流
// 入队；启动时补齐当前 best 缺失的对手（覆盖「评测配置新增对手」「重启后继续」）。
// 下发：一次任务即含全部局数（不做累加），因此重下发只是重做同一件事 —— 结果按
// (network, spec) 覆盖，天然幂等。
// 判据：同一对手的胜率序列上，连续 EvalNoProgressN 次「提升 < EvalNoProgressEps」
// （用户口径：不设绝对阈值，只看趋势）→ 置位 should_stop，trainer 轮询后优雅停止。

const (
	// evalTaskStaleAfter：eval 任务超过该时长未上报即视为失效（worker 掉线 / 版本过旧
	// 不认 TASK_EVAL）。失效即释放 (network, spec) 的在飞名额。
	evalTaskStaleAfter = 15 * time.Minute
	// evalMaxAttempts：同一 (network, spec) 连续下发未上报的次数上限；超过即丢弃该任务
	// 并告警 —— 否则旧版 worker 会让 eval 任务永久占住 GetTask 队首、把自对弈饿死。
	evalMaxAttempts = 3
)

// pendingEval 是一条待下发的评估任务：被测网络 × 对手标识。
//
// 留在队列里直到上报成功：Attempts 记录「已下发但超期未上报」的次数，用于丢弃
// 无法完成的评估（见 evalMaxAttempts），避免队首卡死。
type pendingEval struct {
	NetworkSha string
	Spec       string
	Attempts   int
	CreatedAt  time.Time
}

// 支持的内建对手标识（与 banqi-collector 的 rule_opponents.rs 保持同一套写法）。
const (
	EvalOpponentRandom       = "random"
	EvalOpponentCaptureFirst = "rule:capture_first"
	EvalOpponentRevealFirst  = "rule:reveal_first"
)

// DefaultEvalOpponents 默认阶梯（由弱到强）：随机 → 优先翻棋 → 优先吃子。
// 优先吃子必须在内——它是暴露「练了很久却打不过 3 行启发式」的那个对手。
var DefaultEvalOpponents = []string{
	EvalOpponentRandom,
	EvalOpponentRevealFirst,
	EvalOpponentCaptureFirst,
}

// ValidateEvalOpponents 校验对手标识；启动即失败，避免每条任务下发时才发现配置写错。
func ValidateEvalOpponents(specs []string) error {
	for _, spec := range specs {
		s := strings.TrimSpace(spec)
		if s == "" {
			continue
		}
		switch s {
		case EvalOpponentRandom, EvalOpponentCaptureFirst, EvalOpponentRevealFirst:
		default:
			return fmt.Errorf("未知评估对手 %q（可选 %s / %s / %s）",
				s, EvalOpponentRandom, EvalOpponentRevealFirst, EvalOpponentCaptureFirst)
		}
	}
	return nil
}

// onNewBest best 指针变更后调用：按 EvalEveryNPromotions 节流为各配置对手入队评估。
func (s *Server) onNewBest(ctx context.Context, sha string) {
	if !s.cfg.EvalEnabled || len(s.cfg.EvalOpponents) == 0 || sha == "" {
		return
	}
	every := s.cfg.EvalEveryNPromotions
	if every < 1 {
		every = 1
	}
	s.mu.Lock()
	s.evalPromotions++
	due := s.evalPromotions%every == 0
	s.mu.Unlock()
	if !due {
		return
	}
	if n := s.enqueueEval(sha, s.cfg.EvalOpponents); n > 0 {
		log.Printf("[eval] 入队 network=%s 对手=%v（%d 条；节流 1/%d 次晋级；模式=%s）",
			sha, s.cfg.EvalOpponents, n, every, s.evalModeName())
	}
}

// ensureBestEvaluated 启动时补齐当前 best 缺失的评估（新对手 / 重启后继续）。
func (s *Server) ensureBestEvaluated(ctx context.Context) {
	if !s.cfg.EvalEnabled {
		return
	}
	best, err := s.store.GetBest(ctx)
	if err != nil {
		log.Printf("[eval] ⚠️ 启动补齐：读取 best 失败: %v", err)
		return
	}
	if best == nil {
		return
	}
	var missing []string
	for _, spec := range s.cfg.EvalOpponents {
		spec = strings.TrimSpace(spec)
		if spec == "" {
			continue
		}
		row, err := s.store.GetEvalResult(ctx, best.Sha, spec)
		if err != nil {
			log.Printf("[eval] ⚠️ 启动补齐：查询 %s/%s 失败: %v", best.Sha, spec, err)
			continue
		}
		if row == nil {
			missing = append(missing, spec)
		}
	}
	if len(missing) == 0 {
		return
	}
	if n := s.enqueueEval(best.Sha, missing); n > 0 {
		log.Printf("[eval] 启动补齐当前 best=%s 的缺失评估：%v", best.Sha, missing)
	}
}

// enqueueEval 为指定网络的各对手入队评估任务；已在队/已下发的 (network, spec) 跳过。
// 返回新入队条数。
func (s *Server) enqueueEval(sha string, specs []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, spec := range specs {
		spec = strings.TrimSpace(spec)
		if spec == "" || s.evalQueuedLocked(sha, spec) {
			continue
		}
		s.evals = append(s.evals, &pendingEval{NetworkSha: sha, Spec: spec, CreatedAt: time.Now()})
		n++
	}
	return n
}

// evalQueuedLocked 判断 (network, spec) 是否已在待下发队列中（调用方须持锁）。
func (s *Server) evalQueuedLocked(sha, spec string) bool {
	for _, job := range s.evals {
		if job.NetworkSha == sha && job.Spec == spec {
			return true
		}
	}
	return false
}

// claimEvalTask 原子地认领队首可下发的评估任务：在飞检查 + 记一次下发 + 登记在飞任务
// 全部在同一把锁内完成，并返回（任务, 任务 id）；无待办或队首在飞时返回 (nil, "")。
//
// **必须原子**：若把「检查在飞」与「登记在飞」分两次加锁（peek + register），两个
// 并发 GetTask（多 worker 场景）会把同一 (network, spec) 重复下发。
//
// 队首连续 evalMaxAttempts 次下发未上报即丢弃并告警：旧版 worker 不认识 TASK_EVAL
// （会安全降级为「无任务」），若一直重试会把自对弈饿死。
func (s *Server) claimEvalTask(workerID string) (*pendingEval, string) {
	if !s.cfg.EvalEnabled {
		return nil, ""
	}
	taskID, err := newID()
	if err != nil {
		log.Printf("[eval] ⚠️ 生成任务 id 失败: %v", err)
		return nil, ""
	}
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.evals) > 0 {
		job := s.evals[0]
		if job.Attempts >= evalMaxAttempts {
			s.evals = s.evals[1:]
			log.Printf("[eval] ⚠️ 丢弃评估任务 network=%s opponent=%s：连续 %d 次下发未上报"+
				"（collector 版本过旧不认 TASK_EVAL？或 worker 反复掉线）",
				job.NetworkSha, job.Spec, job.Attempts)
			continue
		}
		if s.evalInFlightLocked(job.NetworkSha, job.Spec, now) {
			return nil, ""
		}
		job.Attempts++
		s.tasks[taskID] = &runningTask{
			Kind:         pb.TaskKind_TASK_EVAL,
			NetworkSha:   job.NetworkSha,
			OpponentSpec: job.Spec,
			Games:        s.cfg.EvalGames,
			WorkerID:     workerID,
			CreatedAt:    now,
		}
		return job, taskID
	}
	return nil, ""
}

// releaseEvalClaim 组装失败时释放已登记的在飞任务（把名额还回去，队列保留待下轮重试）。
func (s *Server) releaseEvalClaim(taskID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.tasks, taskID)
}

// finishEvalTask 收尾一次评估：移出待办队列 **且** 释放在飞认领，在同一把锁内完成。
//
// 顺序同样关键：若先释放认领、后移出队列（两者之间还有 DB 往返），那块窗口里
// 「任务不在飞 + 待办仍在队列」会让 GetTask 再次下发同一 (network, spec) ——
// 实测复现过：同一次评测被下发两次、两条结果互相覆盖。
func (s *Server) finishEvalTask(taskID, sha, spec string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, job := range s.evals {
		if job.NetworkSha == sha && job.Spec == spec {
			s.evals = append(s.evals[:i], s.evals[i+1:]...)
			break
		}
	}
	if t, ok := s.tasks[taskID]; ok {
		t.Done = true
	}
}

// evalInFlightLocked 判断 (network, spec) 是否已有在飞的 eval 任务（调用方须持锁）。
// 超过 evalTaskStaleAfter 未上报的视为失效（worker 掉线），不阻塞重下发。
func (s *Server) evalInFlightLocked(sha, spec string, now time.Time) bool {
	for _, t := range s.tasks {
		if t.Kind == pb.TaskKind_TASK_EVAL && !t.Done &&
			t.NetworkSha == sha && t.OpponentSpec == spec &&
			now.Sub(t.CreatedAt) < evalTaskStaleAfter {
			return true
		}
	}
	return false
}

// PendingEval 待下发评估任务数（WebUI / 状态接口用）。
func (s *Server) PendingEval() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.evals)
}

// consecutiveNoProgress 统计序列末尾「连续提升不足 eps」的次数。
//
// 判据的唯一定义处：judgeEvalProgress（停机判定）与 EvalProgressFor（展示）共用，
// 避免界面显示的「无提升次数」与真正用于停机的数字各算一套。
func consecutiveNoProgress(rates []float64, eps float64) int {
	stale := 0
	for i := 1; i < len(rates); i++ {
		if rates[i]-rates[i-1] < eps {
			stale++
		} else {
			stale = 0
		}
	}
	return stale
}

// EvalProgress 单对手的趋势摘要（WebUI / API 展示用）。
type EvalProgress struct {
	OpponentSpec  string
	Versions      int
	NoProgress    int
	LatestWinRate float64
	LatestSha     string
}

// EvalProgressFor 计算指定对手的趋势摘要；该对手尚无评测记录时 ok=false。
// window<=0 时按停机判据的窗口（EvalNoProgressN+2）取值。
func (s *Server) EvalProgressFor(ctx context.Context, spec string, window int) (EvalProgress, bool) {
	if window <= 0 {
		window = s.cfg.EvalNoProgressN + 2
		if window < 2 {
			window = 2
		}
	}
	trend, err := s.store.EvalTrend(ctx, spec, window)
	if err != nil || len(trend) == 0 {
		return EvalProgress{}, false
	}
	rates := make([]float64, len(trend))
	for i, r := range trend {
		rates[i] = r.WinRate()
	}
	return EvalProgress{
		OpponentSpec:  spec,
		Versions:      len(trend),
		NoProgress:    consecutiveNoProgress(rates, s.cfg.EvalNoProgressEps),
		LatestWinRate: rates[len(rates)-1],
		LatestSha:     trend[len(trend)-1].NetworkSha,
	}, true
}

// evalModeName 评估模式的可读名（日志用）。
func (s *Server) evalModeName() string {
	if s.cfg.EvalMctsSims <= 0 {
		return "纯策略 argmax"
	}
	return fmt.Sprintf("MCTS %d sims", s.cfg.EvalMctsSims)
}

// evalTask 组装评估任务：被测网络 vs 指定对手标识。
//
// 对手为规则/内建策略时不存在网络文件，故不签发 opponent_key / opponent_url；
// 搜索模式由 params.mcts_sims 表达（0 = 纯策略 argmax，>0 = MCTS 模拟数）。
func (s *Server) evalTask(ctx context.Context, taskID string, job *pendingEval, req *pb.TaskRequest) (*pb.TaskResponse, error) {
	net, err := s.store.GetNetwork(ctx, job.NetworkSha)
	if err != nil {
		return nil, fmt.Errorf("get eval network %s: %w", job.NetworkSha, err)
	}
	if net == nil {
		return nil, fmt.Errorf("eval network not found sha=%s", job.NetworkSha)
	}
	networkKey := r2.NetworkKey(net.Sha, net.Format)
	resp := &pb.TaskResponse{
		TaskId:           taskID,
		Kind:             pb.TaskKind_TASK_EVAL,
		NetworkSha:       job.NetworkSha,
		NetworkShaRemote: job.NetworkSha,
		OpponentSpec:     job.Spec,
		Games:            int32(s.cfg.EvalGames),
		NetworkKey:       networkKey,
		Params: &pb.SelfPlayParams{
			Variant: s.cfg.Variant,
			// 0 = 纯策略（无搜索）；>0 = 该次评估的搜索深度
			MctsSims: int32(s.cfg.EvalMctsSims),
			// 评估不产数据，但字段留 ResNet：worker 的能力校验以该字段判定可消费类别
			DataKind: pb.DataKind_DATA_RESNET,
		},
	}
	if req.CurrentNetwork != job.NetworkSha {
		url, err := s.r2.PresignGet(ctx, networkKey)
		if err != nil {
			return nil, fmt.Errorf("presign eval network %s: %w", networkKey, err)
		}
		resp.NetworkUrl = url
	}
	return resp, nil
}

// reportEvalResult 落库一次评估结果，并更新「无提升」判据。
//
// 部分结果不劣化已有结果：同 (network, spec) 只在本次局数不少于已有行时才覆盖
// （重下发 / worker 中途失败可能只带回部分局数）。
func (s *Server) reportEvalResult(ctx context.Context, task *runningTask, req *pb.MatchResult) (*pb.MatchResultAck, error) {
	spec := strings.TrimSpace(req.OpponentSpec)
	if spec == "" {
		spec = task.OpponentSpec
	}
	// 收尾放在最前，且「移出待办队列」与「释放在飞认领」同锁完成（见 finishEvalTask）：
	// 先把两者一起清掉，后续 DB 写入期间不会再被 GetTask 重复下发。
	s.finishEvalTask(req.TaskId, task.NetworkSha, spec)

	n := int(req.Games)
	if n <= 0 {
		log.Printf("[eval] ⚠️ 上报 0 局，忽略 network=%s opponent=%s", task.NetworkSha, spec)
		return &pb.MatchResultAck{Accepted: true, Message: "eval_zero_games_ignored"}, nil
	}
	prev, err := s.store.GetEvalResult(ctx, task.NetworkSha, spec)
	if err != nil {
		return nil, err
	}
	if prev != nil && prev.NumGames > n {
		log.Printf("[eval] 忽略局数更少的评估结果 network=%s opponent=%s games=%d < 已有 %d",
			task.NetworkSha, spec, n, prev.NumGames)
		return &pb.MatchResultAck{Accepted: true, Message: "eval_partial_result_ignored"}, nil
	}

	row := store.EvalResult{
		NetworkSha:   task.NetworkSha,
		OpponentSpec: spec,
		Wins:         int(req.Wins),
		Draws:        int(req.Draws),
		Losses:       int(req.Losses),
		NumGames:     n,
		AvgMoves:     float64(req.AvgMoves),
		CreatedAt:    time.Now(),
	}
	if err := s.store.UpsertEvalResult(ctx, row); err != nil {
		return nil, err
	}
	log.Printf("[eval] ✅ network=%s opponent=%s %d 局 胜%d 平%d 负%d 胜率%.1f%% 步均%.1f",
		task.NetworkSha, spec, n, row.Wins, row.Draws, row.Losses, 100*row.WinRate(), row.AvgMoves)
	s.judgeEvalProgress(ctx, spec)
	return &pb.MatchResultAck{Accepted: true, MatchConcluded: true, Promoted: false}, nil
}

// judgeEvalProgress 按「连续 N 次无提升」判据决定是否置位 should_stop。
//
// 序列取自 eval_results 中同一对手的历次评估（按写入顺序）。提升 < eps 记为无提升；
// 连续次数达到 EvalNoProgressN 即判定停滞。EvalNoProgressN<=0 时关闭判停（只观测）。
func (s *Server) judgeEvalProgress(ctx context.Context, spec string) {
	n := s.cfg.EvalNoProgressN
	if !s.cfg.EvalEnabled || n <= 0 {
		return
	}
	trend, err := s.store.EvalTrend(ctx, spec, n+2)
	if err != nil {
		log.Printf("[eval] ⚠️ 读取趋势失败 opponent=%s: %v", spec, err)
		return
	}
	if len(trend) < 2 {
		return
	}
	eps := s.cfg.EvalNoProgressEps
	rates := make([]float64, len(trend))
	for i, r := range trend {
		rates[i] = r.WinRate()
	}
	stale := consecutiveNoProgress(rates, eps)
	latest := trend[len(trend)-1]
	log.Printf("[eval] 趋势 opponent=%s 已评测版本=%d 连续无提升=%d/%d（eps=%.1fpt，最新胜率 %.1f%%）",
		spec, len(trend), stale, n, eps*100, 100*latest.WinRate())
	if stale < n {
		return
	}
	if s.ctl.ShouldStop() {
		return // 已置位：不重复告警
	}
	reason := fmt.Sprintf("eval_no_progress: opponent=%s 连续 %d 次提升 < %.1fpt（最新胜率 %.1f%%）",
		spec, stale, eps*100, 100*latest.WinRate())
	if err := s.ctl.SetStop(ctx, reason); err != nil {
		log.Printf("[eval] ⚠️ 置位 should_stop 失败: %v", err)
		return
	}
	log.Printf("[eval] 🏁 触发停机：%s", reason)
}
