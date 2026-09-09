package sprt

import (
	"math"
	"testing"
)

func TestJudgeStronger(t *testing.T) {
	// 明显更强：WW 多
	p := Pentanomial{10, 40, 90, 120, 140}
	v, llr, err := Judge(p, 0, 30, NewBounds(0.05, 0.05))
	if err != nil {
		t.Fatalf("judge: %v", err)
	}
	if v != AcceptH1 {
		t.Fatalf("expected accept, got %v (llr=%f)", v, llr)
	}
	if math.IsNaN(llr) || math.IsInf(llr, 0) {
		t.Fatalf("bad llr %f", llr)
	}
}

func TestJudgeWeaker(t *testing.T) {
	p := Pentanomial{140, 120, 90, 40, 10}
	v, _, err := Judge(p, 0, 30, NewBounds(0.05, 0.05))
	if err != nil {
		t.Fatalf("judge: %v", err)
	}
	if v != RejectH0 {
		t.Fatalf("expected reject, got %v", v)
	}
}

func TestJudgeEven(t *testing.T) {
	// 55% 胜率对局，处于 elo0 与 elo1 之间：打满前应继续
	p := Pentanomial{20, 45, 70, 45, 20}
	v, _, err := Judge(p, 0, 30, NewBounds(0.05, 0.05))
	if err != nil {
		t.Fatalf("judge: %v", err)
	}
	if v != Continue {
		t.Fatalf("expected continue, got %v", v)
	}
}

func TestEmpty(t *testing.T) {
	if _, err := LLR(Pentanomial{}, 0, 30); err == nil {
		t.Fatal("expected error on empty pentanomial")
	}
}
