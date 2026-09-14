package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

func (s *Store) TouchWorker(ctx context.Context, id string, threads, completedGames int, clientVersion string, memoryMb int64) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO workers (id, last_seen, threads, completed_games, client_version, memory_mb) VALUES (?,?,?,?,?,?)
		ON CONFLICT(id) DO UPDATE SET last_seen=excluded.last_seen, threads=excluded.threads, completed_games=excluded.completed_games,
		client_version=excluded.client_version, memory_mb=excluded.memory_mb`,
		id, time.Now().Unix(), threads, completedGames, clientVersion, memoryMb)
	if err != nil {
		return fmt.Errorf("touch worker %s: %w", id, err)
	}
	return nil
}

// WorkerVersion 返回该 worker 最近一次上报的版本声明
func (s *Store) WorkerVersion(ctx context.Context, id string) (string, error) {
	var v string
	err := s.db.QueryRowContext(ctx, `SELECT client_version FROM workers WHERE id=?`, id).Scan(&v)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("worker version %s: %w", id, err)
	}
	return v, nil
}

func (s *Store) ListWorkers(ctx context.Context) ([]Worker, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, last_seen, threads, completed_games, client_version, memory_mb
		FROM workers ORDER BY last_seen DESC`)
	if err != nil {
		return nil, fmt.Errorf("list workers: %w", err)
	}
	defer rows.Close()
	var out []Worker
	for rows.Next() {
		var w Worker
		var lastSeen int64
		if err := rows.Scan(&w.ID, &lastSeen, &w.Threads, &w.CompletedGames, &w.ClientVersion, &w.MemoryMb); err != nil {
			return nil, fmt.Errorf("scan worker row: %w", err)
		}
		w.LastSeen = time.Unix(lastSeen, 0)
		out = append(out, w)
	}
	return out, rows.Err()
}
