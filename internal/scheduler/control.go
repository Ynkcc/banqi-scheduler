package scheduler

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"banqi/server/internal/store"
	pb "banqi/server/pb"
)

const (
	settingPauseSelfPlay   = "pause_self_play"
	settingInitialRevealed = "initial_revealed"
	settingDataKind        = "data_kind"
	// 绝对强度评估判停（由 internal/scheduler/eval.go 的判据写入，跨重启保留）
	settingEvalShouldStop = "eval_should_stop"
	settingEvalStopReason = "eval_stop_reason"
)

// DataKindName 返回数据类别的稳定标识（配置项 / settings / WebUI 统一用它）。
func DataKindName(k pb.DataKind) string {
	switch k {
	case pb.DataKind_DATA_NNUE:
		return "nnue"
	default:
		return "resnet"
	}
}

// ParseDataKind 解析数据类别标识；未知取值报错（不静默回退，避免配错却毫无提示）。
func ParseDataKind(s string) (pb.DataKind, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "resnet", "onnx":
		return pb.DataKind_DATA_RESNET, nil
	case "nnue":
		return pb.DataKind_DATA_NNUE, nil
	default:
		return pb.DataKind_DATA_RESNET, fmt.Errorf("未知数据类别 %q（可选 resnet / nnue）", s)
	}
}

// Control 是调度器运行时可控状态（WebUI 写入），落库 settings 表并跨重启保留。
// 环境变量只作为库中无记录时的初始值。
//
// stopped/stopReason 例外：它不是人工开关，而是由绝对强度判据（eval.go）写入的
// **派生停机信号**，用于让 trainer 优雅收尾（GetInfoReply.should_stop）。跨重启保留是
// 有意的——重启不该成为绕过判据的手段；确认要续训时由 API/WebUI 显式清除。
type Control struct {
	mu              sync.RWMutex
	store           *store.Store
	paused          bool
	initialRevealed int
	dataKind        pb.DataKind
	stopped         bool
	stopReason      string
}

func loadControl(ctx context.Context, st *store.Store, defaultInitialRevealed int, defaultDataKind pb.DataKind) (*Control, error) {
	c := &Control{store: st, initialRevealed: defaultInitialRevealed, dataKind: defaultDataKind}
	paused, err := st.GetSetting(ctx, settingPauseSelfPlay)
	if err != nil {
		return nil, err
	}
	c.paused = paused == "1"
	revealed, err := st.GetSetting(ctx, settingInitialRevealed)
	if err != nil {
		return nil, err
	}
	if revealed != "" {
		n, err := strconv.Atoi(revealed)
		if err != nil {
			return nil, fmt.Errorf("setting %s=%q is not an integer: %w", settingInitialRevealed, revealed, err)
		}
		c.initialRevealed = n
	}
	kind, err := st.GetSetting(ctx, settingDataKind)
	if err != nil {
		return nil, err
	}
	if kind != "" {
		if c.dataKind, err = ParseDataKind(kind); err != nil {
			return nil, fmt.Errorf("setting %s=%q: %w", settingDataKind, kind, err)
		}
	}
	stopped, err := st.GetSetting(ctx, settingEvalShouldStop)
	if err != nil {
		return nil, err
	}
	c.stopped = stopped == "1"
	reason, err := st.GetSetting(ctx, settingEvalStopReason)
	if err != nil {
		return nil, err
	}
	c.stopReason = reason
	return c, nil
}

func (c *Control) Paused() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.paused
}

func (c *Control) SetPaused(ctx context.Context, paused bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	value := "0"
	if paused {
		value = "1"
	}
	if err := c.store.SetSetting(ctx, settingPauseSelfPlay, value); err != nil {
		return err
	}
	c.paused = paused
	return nil
}

func (c *Control) InitialRevealed() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.initialRevealed
}

func (c *Control) SetInitialRevealed(ctx context.Context, n int) error {
	if n < 0 {
		return fmt.Errorf("initial_revealed must be >= 0, got %d", n)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.store.SetSetting(ctx, settingInitialRevealed, strconv.Itoa(n)); err != nil {
		return err
	}
	c.initialRevealed = n
	return nil
}

// DataKind 当前自对弈任务要产的数据类别（下发给 worker）。
func (c *Control) DataKind() pb.DataKind {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.dataKind
}

func (c *Control) SetDataKind(ctx context.Context, kind pb.DataKind) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.store.SetSetting(ctx, settingDataKind, DataKindName(kind)); err != nil {
		return err
	}
	c.dataKind = kind
	return nil
}

// ShouldStop trainer 是否应优雅停止（由绝对强度判据置位）。
func (c *Control) ShouldStop() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stopped
}

// StopReason 停机原因（未停机时为空串）。
func (c *Control) StopReason() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stopReason
}

// SetStop 置位停机信号与原因（幂等；重复调用覆盖原因）。
func (c *Control) SetStop(ctx context.Context, reason string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.store.SetSetting(ctx, settingEvalShouldStop, "1"); err != nil {
		return err
	}
	if err := c.store.SetSetting(ctx, settingEvalStopReason, reason); err != nil {
		return err
	}
	c.stopped = true
	c.stopReason = reason
	return nil
}

// ClearStop 清除停机信号（确认要续训时由 API/WebUI 显式调用）。
func (c *Control) ClearStop(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.store.SetSetting(ctx, settingEvalShouldStop, "0"); err != nil {
		return err
	}
	if err := c.store.SetSetting(ctx, settingEvalStopReason, ""); err != nil {
		return err
	}
	c.stopped = false
	c.stopReason = ""
	return nil
}
