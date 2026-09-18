package scheduler

import (
	"context"
	"encoding/json"
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
	// trainConfig 训练配置覆盖项（字段名 → 值字符串），与 settingTrainConfig 落库同步。
	// 只含调度器显式设置过的项；trainer 未收到的字段保持本地 YAML 的值。
	trainConfig map[string]string
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
	// 训练配置覆盖项：库中为 JSON 对象。重启后重新归一校验一次——下发端不再校验，
	// 若库里被手改成非法值（字段名写错 / 超范围），必须在这里拦住而不是带病下发。
	c.trainConfig = map[string]string{}
	rawTrainConfig, err := st.GetSetting(ctx, settingTrainConfig)
	if err != nil {
		return nil, err
	}
	if rawTrainConfig != "" {
		var stored map[string]string
		if err := json.Unmarshal([]byte(rawTrainConfig), &stored); err != nil {
			return nil, fmt.Errorf("setting %s=%q is not a JSON object: %w", settingTrainConfig, rawTrainConfig, err)
		}
		if c.trainConfig, err = normalizeTrainConfig(stored); err != nil {
			return nil, fmt.Errorf("setting %s: %w", settingTrainConfig, err)
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

// TrainConfig 当前训练配置覆盖项（副本；WebUI 与 GetTrainConfig 读取）。
func (c *Control) TrainConfig() map[string]string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]string, len(c.trainConfig))
	for name, value := range c.trainConfig {
		out[name] = value
	}
	return out
}

// SetTrainConfig 全量替换训练配置覆盖项。先归一校验再落库，避免把半套非法配置写进
// settings（loadControl 会因它直接启动失败）。空映射 = 清空覆盖，回落 trainer 本地配置。
func (c *Control) SetTrainConfig(ctx context.Context, overrides map[string]string) error {
	normalized, err := normalizeTrainConfig(overrides)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return fmt.Errorf("序列化训练配置: %w", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.store.SetSetting(ctx, settingTrainConfig, string(encoded)); err != nil {
		return err
	}
	c.trainConfig = normalized
	return nil
}
