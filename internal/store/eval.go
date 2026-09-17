package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// EvalResult 是一次「绝对强度评估」的结果（被测网络 × 对手标识）。
//
// 与 matches 表刻意分离，原因有二：
//  1. 对手可以是规则策略（如 rule:capture_first），不存在对应的 networks 行，
//     而 matches.opponent 是 REFERENCES networks(sha) 外键；
//  2. 评估结果不参与 gatekeeper 晋级判定，只用于趋势观测与「连续 N 次无提升」判据。
type EvalResult struct {
	ID           int64
	NetworkSha   string
	OpponentSpec string
	Wins         int
	Draws        int
	Losses       int
	NumGames     int
	AvgMoves     float64
	CreatedAt    time.Time
}

// WinRate 胜率（总局数为 0 时返回 0）。
func (r EvalResult) WinRate() float64 {
	if r.NumGames <= 0 {
		return 0
	}
	return float64(r.Wins) / float64(r.NumGames)
}

// evalResultColumns eval_results 表的统一列序（scanEvalResult 与各查询共用）。
const evalResultColumns = `id, network_sha, opponent_spec, wins, draws, losses, num_games, avg_moves, created_at`

// UpsertEvalResult 写入一次评估结果。
//
// 同一 (network_sha, opponent_spec) 只保留一条：同版本的重复评估（上报重试、提高局数
// 复测）覆盖旧值，避免趋势序列里出现同一版本的重复点、把「无提升」判据带偏。
func (s *Store) UpsertEvalResult(ctx context.Context, r EvalResult) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO eval_results (network_sha, opponent_spec, wins, draws, losses, num_games, avg_moves, created_at)
		VALUES (?,?,?,?,?,?,?,?)
		ON CONFLICT(network_sha, opponent_spec) DO UPDATE SET
			wins=excluded.wins, draws=excluded.draws, losses=excluded.losses,
			num_games=excluded.num_games, avg_moves=excluded.avg_moves,
			created_at=excluded.created_at`,
		r.NetworkSha, r.OpponentSpec, r.Wins, r.Draws, r.Losses, r.NumGames, r.AvgMoves, r.CreatedAt.Unix())
	if err != nil {
		return fmt.Errorf("upsert eval result %s/%s: %w", r.NetworkSha, r.OpponentSpec, err)
	}
	return nil
}

// ListEvalResults 返回最近的评估结果（新→旧）；limit<=0 视为不限。
func (s *Store) ListEvalResults(ctx context.Context, limit int) ([]EvalResult, error) {
	query := `SELECT ` + evalResultColumns + ` FROM eval_results ORDER BY id DESC`
	args := []any{}
	if limit > 0 {
		query += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list eval results: %w", err)
	}
	defer rows.Close()
	return scanEvalResults(rows)
}

// EvalTrend 返回单一对手标识下最近 limit 次评估，按写入顺序升序（老→新）。
//
// 升序是「连续 N 次无提升」判据的输入：判据需要沿版本序列逐对比较胜率。
func (s *Store) EvalTrend(ctx context.Context, opponentSpec string, limit int) ([]EvalResult, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+evalResultColumns+` FROM (
			SELECT `+evalResultColumns+` FROM eval_results
			WHERE opponent_spec=? ORDER BY id DESC LIMIT ?
		) ORDER BY id ASC`, opponentSpec, limit)
	if err != nil {
		return nil, fmt.Errorf("eval trend %s: %w", opponentSpec, err)
	}
	defer rows.Close()
	return scanEvalResults(rows)
}

// ListEvalOpponents 返回出现过的对手标识（按首次出现顺序），供 WebUI 分组展示。
func (s *Store) ListEvalOpponents(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT opponent_spec FROM eval_results GROUP BY opponent_spec ORDER BY MIN(id)`)
	if err != nil {
		return nil, fmt.Errorf("list eval opponents: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var spec string
		if err := rows.Scan(&spec); err != nil {
			return nil, fmt.Errorf("scan eval opponent: %w", err)
		}
		out = append(out, spec)
	}
	return out, rows.Err()
}

// GetEvalResult 取指定 (网络, 对手) 的一条结果；不存在返回 nil。
func (s *Store) GetEvalResult(ctx context.Context, networkSha, opponentSpec string) (*EvalResult, error) {
	r := &EvalResult{}
	var created int64
	err := s.db.QueryRowContext(ctx,
		`SELECT `+evalResultColumns+` FROM eval_results WHERE network_sha=? AND opponent_spec=?`,
		networkSha, opponentSpec).
		Scan(&r.ID, &r.NetworkSha, &r.OpponentSpec, &r.Wins, &r.Draws, &r.Losses,
			&r.NumGames, &r.AvgMoves, &created)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	r.CreatedAt = time.Unix(created, 0)
	if err != nil {
		return nil, fmt.Errorf("get eval result %s/%s: %w", networkSha, opponentSpec, err)
	}
	return r, nil
}

func scanEvalResults(rows *sql.Rows) ([]EvalResult, error) {
	var out []EvalResult
	for rows.Next() {
		var r EvalResult
		var created int64
		if err := rows.Scan(&r.ID, &r.NetworkSha, &r.OpponentSpec, &r.Wins, &r.Draws,
			&r.Losses, &r.NumGames, &r.AvgMoves, &created); err != nil {
			return nil, fmt.Errorf("scan eval result row: %w", err)
		}
		r.CreatedAt = time.Unix(created, 0)
		out = append(out, r)
	}
	return out, rows.Err()
}
