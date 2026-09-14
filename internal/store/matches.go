package store

import (
	"context"
	"database/sql"
	"fmt"
)

// matchColumns matches 表的统一列序（scanMatch 与 ListMatches 共用）。
const matchColumns = `id, candidate, opponent, status, pair_ll, pair_ld, pair_dd, pair_dw, pair_ww, num_games, target_games`

func (s *Store) CreateMatch(ctx context.Context, candidate, opponent string, targetGames int) (int64, error) {
	res, err := s.db.ExecContext(ctx, `INSERT INTO matches (candidate, opponent, target_games) VALUES (?,?,?)`,
		candidate, opponent, targetGames)
	if err != nil {
		return 0, fmt.Errorf("create match %s vs %s: %w", candidate, opponent, err)
	}
	return res.LastInsertId()
}

// PendingMatch 取最早一个未完结的 gatekeeper 对打
func (s *Store) PendingMatch(ctx context.Context) (*Match, error) {
	return s.scanMatch(ctx, `SELECT `+matchColumns+` FROM matches WHERE status='running' ORDER BY id LIMIT 1`, nil)
}

func (s *Store) GetMatch(ctx context.Context, id int64) (*Match, error) {
	return s.scanMatch(ctx, `SELECT `+matchColumns+` FROM matches WHERE id=?`, id)
}

func (s *Store) scanMatch(ctx context.Context, query string, arg any) (*Match, error) {
	m := &Match{}
	err := s.db.QueryRowContext(ctx, query, arg).
		Scan(&m.ID, &m.Candidate, &m.Opponent, &m.Status,
			&m.Pairs[0], &m.Pairs[1], &m.Pairs[2], &m.Pairs[3], &m.Pairs[4],
			&m.NumGames, &m.TargetGames)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("scan match: %w", err)
	}
	return m, nil
}

func (s *Store) UpdateMatchResult(ctx context.Context, id int64, pairs [5]int, numGames int, status string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE matches SET pair_ll=?, pair_ld=?, pair_dd=?, pair_dw=?, pair_ww=?, num_games=?, status=? WHERE id=?`,
		pairs[0], pairs[1], pairs[2], pairs[3], pairs[4], numGames, status, id)
	if err != nil {
		return fmt.Errorf("update match %d: %w", id, err)
	}
	return nil
}

func (s *Store) ListMatches(ctx context.Context) ([]Match, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+matchColumns+` FROM matches
		ORDER BY CASE status WHEN 'running' THEN 0 ELSE 1 END, id DESC`)
	if err != nil {
		return nil, fmt.Errorf("list matches: %w", err)
	}
	defer rows.Close()
	var out []Match
	for rows.Next() {
		m := Match{}
		if err := rows.Scan(&m.ID, &m.Candidate, &m.Opponent, &m.Status,
			&m.Pairs[0], &m.Pairs[1], &m.Pairs[2], &m.Pairs[3], &m.Pairs[4],
			&m.NumGames, &m.TargetGames); err != nil {
			return nil, fmt.Errorf("scan match row: %w", err)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}
