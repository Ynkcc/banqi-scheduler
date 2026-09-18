package store

import (
	"context"
	"fmt"
	"time"
)

// EvalQueueEntry 是 eval_queue 表的内存投影（启动时回填 s.evals 用）。
type EvalQueueEntry struct {
	NetworkSha string
	Spec       string
	Attempts   int
	CreatedAt  time.Time
}

// LoadEvalQueue 回填进程内待办队列。
//
// 启动时按 (network_sha, opponent_spec) 主键去重加载。若上次进程崩溃前已有 task
// 在飞、DB 行存在但进程内 s.tasks 为空，pending 行还在——重启后 GetTask 会重新下发。
func (s *Store) LoadEvalQueue(ctx context.Context) ([]EvalQueueEntry, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT network_sha, opponent_spec, attempts, created_at
		FROM eval_queue ORDER BY created_at, network_sha, opponent_spec`)
	if err != nil {
		return nil, fmt.Errorf("query eval_queue: %w", err)
	}
	defer rows.Close()
	var out []EvalQueueEntry
	for rows.Next() {
		var e EvalQueueEntry
		var ts int64
		if err := rows.Scan(&e.NetworkSha, &e.Spec, &e.Attempts, &ts); err != nil {
			return nil, err
		}
		e.CreatedAt = time.Unix(ts, 0)
		out = append(out, e)
	}
	return out, rows.Err()
}

// EnqueueEvalQueue INSERT OR IGNORE：同 (network, spec) 已在队列里就 no-op。
// returns true 表示新插入，false 表示已存在。
func (s *Store) EnqueueEvalQueue(ctx context.Context, networkSha, spec string) (inserted bool, err error) {
	res, err := s.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO eval_queue (network_sha, opponent_spec, attempts, created_at) VALUES (?,?,?,?)`,
		networkSha, spec, 0, time.Now().Unix())
	if err != nil {
		return false, fmt.Errorf("insert eval_queue %s/%s: %w", networkSha, spec, err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// DeleteEvalQueue 收尾时移除已处理的待办（finishEvalTask 调用）。
// 缺席视为成功：避免和「先删后插入」竞态产生误导。
func (s *Store) DeleteEvalQueue(ctx context.Context, networkSha, spec string) error {
	if _, err := s.db.ExecContext(ctx,
		`DELETE FROM eval_queue WHERE network_sha = ? AND opponent_spec = ?`, networkSha, spec); err != nil {
		return fmt.Errorf("delete eval_queue %s/%s: %w", networkSha, spec, err)
	}
	return nil
}

// IncEvalQueueAttempts 认领一次任务时 attempts++；上限判定完全在调度层完成，
// 这里只负责原子递增（事务内 select-then-update）。
//
// 上限语义：「超过 maxAttempts 即丢弃」由调用方在 Inc 之前判定——
// 0..maxAttempts-1 共 maxAttempts 次正常下发，第 maxAttempts 次起被丢。
// 若 Inc 与丢弃同事务完成会导致少一次下发，与现有测试契约不符，故分层。
func (s *Store) IncEvalQueueAttempts(ctx context.Context, networkSha, spec string) (attempts int, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	var cur int
	if err := tx.QueryRowContext(ctx,
		`SELECT attempts FROM eval_queue WHERE network_sha = ? AND opponent_spec = ?`,
		networkSha, spec).Scan(&cur); err != nil {
		return 0, fmt.Errorf("read eval_queue attempts: %w", err)
	}
	cur++
	if _, err := tx.ExecContext(ctx,
		`UPDATE eval_queue SET attempts = ? WHERE network_sha = ? AND opponent_spec = ?`,
		cur, networkSha, spec); err != nil {
		return 0, fmt.Errorf("update eval_queue attempts: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return cur, nil
}

// GetEvalQueueAttempts 仅读取 attempts（不修改）；便于测试与重启恢复。
func (s *Store) GetEvalQueueAttempts(ctx context.Context, networkSha, spec string) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx,
		`SELECT attempts FROM eval_queue WHERE network_sha = ? AND opponent_spec = ?`,
		networkSha, spec).Scan(&n)
	if err != nil {
		return 0, err
	}
	return n, nil
}
