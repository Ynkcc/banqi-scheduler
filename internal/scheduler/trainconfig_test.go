package scheduler

import (
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"banqi/server/internal/store"
	pb "banqi/server/pb"
)

func TestNormalizeTrainConfig(t *testing.T) {
	cases := []struct {
		name    string
		in      map[string]string
		want    map[string]string
		wantErr string
	}{
		{
			name: "bool 写法归一",
			in:   map[string]string{"DATA_AUGMENT_ENABLED": " ON ", "RECENT_SAMPLE_ENABLED": "0"},
			want: map[string]string{"DATA_AUGMENT_ENABLED": "true", "RECENT_SAMPLE_ENABLED": "false"},
		},
		{
			name: "float 归一为规范写法",
			in:   map[string]string{"LEARNING_RATE": "1e-3", "EMA_DECAY": "0.999"},
			want: map[string]string{"LEARNING_RATE": "0.001", "EMA_DECAY": "0.999"},
		},
		{
			name: "enum 大小写归一",
			in:   map[string]string{"VALUE_TARGET_MODE": "Game_HP"},
			want: map[string]string{"VALUE_TARGET_MODE": "game_hp"},
		},
		{
			name:    "非白名单字段拒绝",
			in:      map[string]string{"POLICY_TRUNK_INDEPENDENT": "true"},
			wantErr: "不可远程调节",
		},
		{
			name:    "int 越界拒绝",
			in:      map[string]string{"TRAIN_BATCH": "0"},
			wantErr: "必须 >= 1",
		},
		{
			name:    "float 上界拒绝",
			in:      map[string]string{"WEIGHT_DECAY": "2"},
			wantErr: "必须 <= 1",
		},
		{
			name:    "gtZero 拒绝零值",
			in:      map[string]string{"POLICY_TARGET_TEMPERATURE": "0"},
			wantErr: "必须 > 0",
		},
		{
			name:    "bool 未知写法拒绝",
			in:      map[string]string{"DATA_AUGMENT_ENABLED": "maybe"},
			wantErr: "需为布尔值",
		},
		{
			name:    "enum 非法取值拒绝",
			in:      map[string]string{"VALUE_TARGET_MODE": "nope"},
			wantErr: "非法取值",
		},
		{
			name:    "非数值拒绝",
			in:      map[string]string{"LR_DECAY_STEPS": "1e3"},
			wantErr: "不是整数",
		},
		{
			name: "空覆盖合法",
			in:   map[string]string{},
			want: map[string]string{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := normalizeTrainConfig(tc.in)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("want error containing %q, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("normalize: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// 部分非法必须整批拒绝：否则 cfg 会停在半套新值上（比不生效更难排查）。
func TestNormalizeTrainConfigRejectsWholeBatch(t *testing.T) {
	if _, err := normalizeTrainConfig(map[string]string{"LEARNING_RATE": "0.001", "TRAIN_BATCH": "-1"}); err == nil {
		t.Fatal("want error for illegal TRAIN_BATCH alongside legal LEARNING_RATE")
	}
}

func TestTrainConfigFieldsExposesSpec(t *testing.T) {
	fields := TrainConfigFields()
	if len(fields) != len(trainConfigSpecs) {
		t.Fatalf("got %d fields, want %d", len(fields), len(trainConfigSpecs))
	}
	for i := 1; i < len(fields); i++ {
		if fields[i-1].Name >= fields[i].Name {
			t.Fatalf("字段未按升序: %s >= %s", fields[i-1].Name, fields[i].Name)
		}
	}
	byName := map[string]TrainConfigField{}
	for _, f := range fields {
		byName[f.Name] = f
	}
	if f := byName["LEARNING_RATE"]; f.Kind != string(tcFloat) || f.Min != 0 || !f.HasMax || f.Max != 1 {
		t.Fatalf("LEARNING_RATE 描述不符: %+v", f)
	}
	if f := byName["POLICY_TARGET_TEMPERATURE"]; !f.GtZero || f.HasMax {
		t.Fatalf("POLICY_TARGET_TEMPERATURE 描述不符: %+v", f)
	}
	if f := byName["VALUE_TARGET_MODE"]; f.Kind != string(tcEnum) || len(f.Enum) == 0 {
		t.Fatalf("VALUE_TARGET_MODE 描述不符: %+v", f)
	}
}

func TestControlTrainConfigRoundTrip(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "sched.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })

	ctl := &Control{store: st, trainConfig: map[string]string{}}
	if err := ctl.SetTrainConfig(ctx, map[string]string{"LEARNING_RATE": "1e-3"}); err != nil {
		t.Fatalf("SetTrainConfig: %v", err)
	}
	want := map[string]string{"LEARNING_RATE": "0.001"}
	if got := ctl.TrainConfig(); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}

	reloaded, err := loadControl(ctx, st, 0, pb.DataKind_DATA_RESNET)
	if err != nil {
		t.Fatalf("loadControl: %v", err)
	}
	if got := reloaded.TrainConfig(); !reflect.DeepEqual(got, want) {
		t.Fatalf("重启后 got %v, want %v", got, want)
	}

	if err := ctl.SetTrainConfig(ctx, map[string]string{"TRAIN_BATCH": "0"}); err == nil {
		t.Fatal("want error for out-of-range TRAIN_BATCH")
	}
	if got := ctl.TrainConfig(); !reflect.DeepEqual(got, want) {
		t.Fatalf("非法输入不得改动现有覆盖: got %v, want %v", got, want)
	}

	if err := ctl.SetTrainConfig(ctx, map[string]string{}); err != nil {
		t.Fatalf("清空覆盖: %v", err)
	}
	if got := ctl.TrainConfig(); len(got) != 0 {
		t.Fatalf("清空后应无覆盖: %v", got)
	}
}
