package scheduler

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// settingTrainConfig 训练配置覆盖项的落库键：JSON 对象（字段名 → 值字符串）。
// 字段名与 banqi_training.config.Config 的字段一一对应，trainer 经 GetTrainConfig
// 拉取后**就地覆盖**本地 YAML 的对应值；未下发的字段保持 trainer 本地配置。
const settingTrainConfig = "train_config"

// trainConfigKind 覆盖项取值类型：决定规范化方式与 WebUI 表单控件。
type trainConfigKind string

const (
	tcFloat trainConfigKind = "float"
	tcInt   trainConfigKind = "int"
	tcBool  trainConfigKind = "bool"
	tcEnum  trainConfigKind = "enum"
)

// trainConfigSpec 单个可远程调节字段的规格（取值合法性的唯一权威）。
type trainConfigSpec struct {
	kind   trainConfigKind
	min    float64 // 闭下界（float / int）
	max    float64 // 闭上界，仅 hasMax 时校验
	hasMax bool
	gtZero bool     // 必须严格 > 0
	enum   []string // kind = tcEnum 时的合法取值（小写）
}

// trainConfigSpecs 可迁移到调度层的训练配置白名单。
//
// 判据：不改模型结构、不依赖本地路径/设备、且能在训练循环中热更。不在表中的字段
// 一律拒绝下发——结构开关（HEALTH_VALUE_HEAD_ENABLED / VALUE_DIST_* /
// POLICY_TRUNK_INDEPENDENT）改了与旧 checkpoint 不兼容；路径与设备、一次性资源容量
// （MAX_SAMPLE_BUFFER_SIZE / REANALYSIS_POOL_SIZE）在构造期决定且运行中无法恢复。
//
// 与 banqi_training/config.py 的 TRAIN_CONFIG_OVERRIDABLE 必须同步增删。
var trainConfigSpecs = map[string]trainConfigSpec{
	// ---- LR 计划（改动后 trainer 重建余弦调度器并保留进度）----
	"LEARNING_RATE":   {kind: tcFloat, min: 0, max: 1, hasMax: true},
	"MIN_LR":          {kind: tcFloat, min: 0, max: 1, hasMax: true},
	"LR_DECAY_STEPS":  {kind: tcInt, min: 1},
	"LR_DECAY_ROUNDS": {kind: tcInt, min: 0},
	// ---- 训练量 ----
	"TRAIN_BATCH":              {kind: tcInt, min: 1},
	"TRAIN_EPOCHS_PER_ROUND":   {kind: tcInt, min: 1},
	"MIN_NEW_SAMPLES_TO_TRAIN": {kind: tcInt, min: 0},
	"WEIGHT_DECAY":             {kind: tcFloat, min: 0, max: 1, hasMax: true},
	// ---- EMA 与采样 ----
	"EMA_DECAY":               {kind: tcFloat, min: 0, max: 1, hasMax: true},
	"RECENT_SAMPLE_ENABLED":   {kind: tcBool},
	"FAST_SAMPLE_LOSS_WEIGHT": {kind: tcFloat, min: 0, max: 1, hasMax: true},
	// ---- 目标函数（取值域对齐 config.py 与 buffer.py 的构造期校验）----
	"VALUE_TARGET_MODE":          {kind: tcEnum, enum: []string{"mcts", "game", "completed_q", "mixed", "anneal", "game_hp"}},
	"VALUE_TARGET_ANNEAL_ROUNDS": {kind: tcInt, min: 0},
	"VALUE_MIX_GAME_WEIGHT":      {kind: tcFloat, min: 0, max: 1, hasMax: true},
	"POLICY_TARGET_TEMPERATURE":  {kind: tcFloat, gtZero: true},
	"POLICY_TARGET_ACTION_MIX":   {kind: tcFloat, min: 0, max: 1, hasMax: true},
	"HEALTH_LOSS_WEIGHT":         {kind: tcFloat, min: 0, max: 10, hasMax: true},
	"HEALTH_GAUSS_SIGMA":         {kind: tcFloat, gtZero: true, max: 1, hasMax: true},
	"VALUE_GAUSS_SIGMA":          {kind: tcFloat, gtZero: true, max: 1, hasMax: true},
	// ---- 数据增强 ----
	"DATA_AUGMENT_ENABLED":       {kind: tcBool},
	"DATA_AUGMENT_K":             {kind: tcInt, min: 1},
	"DATA_AUGMENT_KEEP_ORIGINAL": {kind: tcBool},
	// ---- 局面重搜节流 ----
	"REANALYSIS_BATCH_POSITIONS":       {kind: tcInt, min: 1},
	"REANALYSIS_SUBMIT_EVERY_N_ROUNDS": {kind: tcInt, min: 1},
	"REANALYSIS_MCTS_SIMS":             {kind: tcInt, min: 0},
	// ---- 运行控制 ----
	"MAX_RUNTIME_SECONDS":      {kind: tcInt, min: 0},
	"SHOULD_STOP_POLL_SECONDS": {kind: tcInt, min: 0},
}

// TrainConfigField 是白名单字段的只读描述：WebUI 据此渲染表单与校验提示，免前端
// 硬编码字段清单与取值范围（唯一权威仍是服务端 normalize，此处仅供表单约束）。
type TrainConfigField struct {
	Name   string   `json:"name"`
	Kind   string   `json:"kind"`
	Min    float64  `json:"min"`
	Max    float64  `json:"max"`
	HasMax bool     `json:"hasMax"`
	GtZero bool     `json:"gtZero"`
	Enum   []string `json:"enum,omitempty"`
}

// TrainConfigFields 按字段名升序返回白名单描述。
func TrainConfigFields() []TrainConfigField {
	names := trainConfigFieldNames()
	out := make([]TrainConfigField, 0, len(names))
	for _, name := range names {
		spec := trainConfigSpecs[name]
		out = append(out, TrainConfigField{
			Name:   name,
			Kind:   string(spec.kind),
			Min:    spec.min,
			Max:    spec.max,
			HasMax: spec.hasMax,
			GtZero: spec.gtZero,
			Enum:   spec.enum,
		})
	}
	return out
}

// trainConfigFieldNames 升序字段名：错误提示与表单排序都依赖稳定顺序。
func trainConfigFieldNames() []string {
	names := make([]string, 0, len(trainConfigSpecs))
	for name := range trainConfigSpecs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// normalizeTrainConfig 校验并规范化「字段名 → 值」覆盖项。非白名单字段直接报错，
// 不静默丢弃——远程配置写错字段名必须立刻可见；取值按 spec 归一后返回。
func normalizeTrainConfig(in map[string]string) (map[string]string, error) {
	out := make(map[string]string, len(in))
	for name, raw := range in {
		spec, ok := trainConfigSpecs[name]
		if !ok {
			return nil, fmt.Errorf("字段 %q 不可远程调节；可调字段: %s",
				name, strings.Join(trainConfigFieldNames(), " "))
		}
		value, err := spec.normalize(name, raw)
		if err != nil {
			return nil, err
		}
		out[name] = value
	}
	return out, nil
}

// normalize 把单个字段的原始字符串解析为规范写法，并校验取值域。
func (s trainConfigSpec) normalize(name, raw string) (string, error) {
	value := strings.TrimSpace(raw)
	switch s.kind {
	case tcBool:
		parsed, err := parseTrainConfigBool(value)
		if err != nil {
			return "", fmt.Errorf("%s=%q: %w", name, raw, err)
		}
		return strconv.FormatBool(parsed), nil
	case tcInt:
		parsed, err := strconv.Atoi(value)
		if err != nil {
			return "", fmt.Errorf("%s=%q 不是整数: %w", name, raw, err)
		}
		if err := s.checkRange(name, float64(parsed)); err != nil {
			return "", err
		}
		return strconv.Itoa(parsed), nil
	case tcFloat:
		parsed, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return "", fmt.Errorf("%s=%q 不是数值: %w", name, raw, err)
		}
		if err := s.checkRange(name, parsed); err != nil {
			return "", err
		}
		return strconv.FormatFloat(parsed, 'g', -1, 64), nil
	case tcEnum:
		lowered := strings.ToLower(value)
		for _, candidate := range s.enum {
			if lowered == candidate {
				return candidate, nil
			}
		}
		return "", fmt.Errorf("%s=%q 非法取值，可选: %s", name, raw, strings.Join(s.enum, " / "))
	default:
		return "", fmt.Errorf("%s: 白名单项取值类型 %q 未知", name, s.kind)
	}
}

// checkRange 校验数值域；错误信息带具体字段与边界，便于 WebUI 直接回显。
func (s trainConfigSpec) checkRange(name string, value float64) error {
	if s.gtZero && value <= 0 {
		return fmt.Errorf("%s 必须 > 0，得到 %g", name, value)
	}
	if value < s.min {
		return fmt.Errorf("%s 必须 >= %g，得到 %g", name, s.min, value)
	}
	if s.hasMax && value > s.max {
		return fmt.Errorf("%s 必须 <= %g，得到 %g", name, s.max, value)
	}
	return nil
}

// parseTrainConfigBool 只接受明确的布尔写法。这里刻意不用 strconv.ParseBool，
// 也不沿用 config.py `_cast_bool` 的「未知值即真」容错：远程下发写错必须报错。
func parseTrainConfigBool(value string) (bool, error) {
	switch strings.ToLower(value) {
	case "1", "true", "yes", "on":
		return true, nil
	case "0", "false", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("需为布尔值（true/false、1/0、yes/no、on/off）")
	}
}
