package sprt

import (
	"fmt"
	"math"
)

// Pentanomial 五项分布计数，顺序：[LL, LD, DD, DW, WW]
// 其中 DD 聚合 ( draw,draw ) 与 ( win,loss )+（loss,win )，
// 与 fishtest/pentanomial (vdbergh) 的约定一致。
type Pentanomial [5]int

func (p Pentanomial) Total() int {
	t := 0
	for _, c := range p {
		t += c
	}
	return t
}

// PairScore 每对（两局）得分，范围 [0,2]；返回 (均值, 每局方差)
func (p Pentanomial) Stats() (mean float64, variancePerGame float64, err error) {
	n := p.Total()
	if n == 0 {
		return 0, 0, fmt.Errorf("pentanomial empty")
	}
	values := []float64{0, 0.5, 1, 1.5, 2}
	sum, sumSq := 0.0, 0.0
	for i, c := range p {
		sum += float64(c) * values[i]
		sumSq += float64(c) * values[i] * values[i]
	}
	mean = sum / float64(n)
	// 成对得分方差（单位：对）
	varPair := (sumSq - float64(n)*mean*mean) / float64(n-1)
	// 每局得分 = 成对得分 / 2，方差同理
	return mean / 2, varPair / 4, nil
}

func scoreFromElo(elo float64) float64 {
	return 1 / (1 + math.Pow(10, -elo/400))
}

// LLR 基于正态近似的广义 SPRT：
//   LLR ≈ N * (s - (s0+s1)/2) * (s1-s0) / σ²
// s 为每局平均得分，σ² 为每局得分方差，s0/s1 由 elo0/elo1 边界换算。
func LLR(p Pentanomial, elo0, elo1 float64) (float64, error) {
	s, variance, err := p.Stats()
	if err != nil {
		return 0, err
	}
	s0 := scoreFromElo(elo0)
	s1 := scoreFromElo(elo1)
	if variance <= 0 {
		// 全胜/全负无方差：按经验下限处理，避免除零
		variance = 0.1
	}
	n := float64(p.Total())
	llr := n * (s - (s0+s1)/2) * (s1 - s0) / variance
	return llr, nil
}

type Bounds struct {
	Lower float64 // ≤ Lower → 拒绝（H0 elo0 成立）
	Upper float64 // ≥ Upper → 接受（H1 elo1 成立）
}

func NewBounds(alpha, beta float64) Bounds {
	return Bounds{
		Lower: math.Log(beta / (1 - alpha)),
		Upper: math.Log((1 - beta) / alpha),
	}
}

type Verdict int

const (
	Continue Verdict = iota
	AcceptH1 // 新网络变强，晋级
	RejectH0 // 新网络未达标，拒绝
)

func (v Verdict) String() string {
	switch v {
	case AcceptH1:
		return "accept"
	case RejectH0:
		return "reject"
	default:
		return "continue"
	}
}

func Judge(p Pentanomial, elo0, elo1 float64, bounds Bounds) (Verdict, float64, error) {
	llr, err := LLR(p, elo0, elo1)
	if err != nil {
		return Continue, 0, err
	}
	switch {
	case llr >= bounds.Upper:
		return AcceptH1, llr, nil
	case llr <= bounds.Lower:
		return RejectH0, llr, nil
	default:
		return Continue, llr, nil
	}
}
