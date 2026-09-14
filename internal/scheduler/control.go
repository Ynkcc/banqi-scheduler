package scheduler

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"banqi/server/internal/store"
)

const (
	settingPauseSelfPlay   = "pause_self_play"
	settingInitialRevealed = "initial_revealed"
)

// Control 是调度器运行时可控状态（WebUI 写入），落库 settings 表并跨重启保留。
// 环境变量只作为库中无记录时的初始值。
type Control struct {
	mu              sync.RWMutex
	store           *store.Store
	paused          bool
	initialRevealed int
}

func loadControl(ctx context.Context, st *store.Store, defaultInitialRevealed int) (*Control, error) {
	c := &Control{store: st, initialRevealed: defaultInitialRevealed}
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
