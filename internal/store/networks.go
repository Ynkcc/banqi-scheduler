package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

const networkColumns = `sha, COALESCE(parent_sha,''), created_at, is_best, status, COALESCE(notes,''), format`

// scanNetworkRow 统一 networks 行扫描，*sql.Row 与 *sql.Rows 通用。
func scanNetworkRow(row interface{ Scan(...any) error }) (*Network, error) {
	n := &Network{}
	var createdAt int64
	if err := row.Scan(&n.Sha, &n.ParentSha, &createdAt, &n.IsBest, &n.Status, &n.Notes, &n.Format); err != nil {
		return nil, err
	}
	n.CreatedAt = time.Unix(createdAt, 0)
	return n, nil
}

func (s *Store) GetBest(ctx context.Context) (*Network, error) {
	n, err := scanNetworkRow(s.db.QueryRowContext(ctx, `SELECT `+networkColumns+` FROM networks WHERE is_best=1 LIMIT 1`))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get best network: %w", err)
	}
	return n, nil
}

func (s *Store) GetNetwork(ctx context.Context, sha string) (*Network, error) {
	n, err := scanNetworkRow(s.db.QueryRowContext(ctx, `SELECT `+networkColumns+` FROM networks WHERE sha=?`, sha))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get network %s: %w", sha, err)
	}
	return n, nil
}

func (s *Store) RegisterNetwork(ctx context.Context, sha, parentSha, notes, format string) (created bool, err error) {
	now := time.Now().Unix()
	res, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO networks (sha, parent_sha, created_at, status, notes, format) VALUES (?,?,?,'candidate',?,?)`,
		sha, parentSha, now, notes, format)
	if err != nil {
		return false, fmt.Errorf("insert network %s: %w", sha, err)
	}
	aff, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("insert network %s rows affected: %w", sha, err)
	}
	return aff > 0, nil
}

// PromoteBest 原子切换 best 指针并标记 candidate 晋级
func (s *Store) PromoteBest(ctx context.Context, sha string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("promote begin: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE networks SET is_best=0, status='archived' WHERE is_best=1`); err != nil {
		return fmt.Errorf("demote old best: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE networks SET is_best=1, status='best' WHERE sha=?`, sha); err != nil {
		return fmt.Errorf("promote %s: %w", sha, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("promote commit: %w", err)
	}
	return nil
}

func (s *Store) UpdateNetworkStatus(ctx context.Context, sha, status string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE networks SET status=? WHERE sha=?`, status, sha)
	if err != nil {
		return fmt.Errorf("update network %s status=%s: %w", sha, status, err)
	}
	return nil
}

func (s *Store) ListNetworks(ctx context.Context) ([]Network, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+networkColumns+` FROM networks ORDER BY is_best DESC, created_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("list networks: %w", err)
	}
	defer rows.Close()
	var out []Network
	for rows.Next() {
		n, err := scanNetworkRow(rows)
		if err != nil {
			return nil, fmt.Errorf("scan network row: %w", err)
		}
		out = append(out, *n)
	}
	return out, rows.Err()
}
