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
type Control struct {
	mu              sync.RWMutex
	store           *store.Store
	paused          bool
	initialRevealed int
	dataKind        pb.DataKind
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
