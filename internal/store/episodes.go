package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// KindAny 表示不过滤数据类别（ListEpisodeKeys 的 kind 参数）。
const KindAny = -1

// ListEpisodeKeys 游标分页列出已登记的 episode 对象键（按登记顺序递增）。
// afterKey 为上次返回的最后一个键，服务端据此解析出该行的 id 再按 id 推进：
// 对象键含随机段，字典序与登记顺序无关，用字典序做游标会永久漏掉后写入的对象。
// kind >= 0 时只返回该数据类别的对象（KindAny = 不过滤）；limit<=0 时取默认 200。
func (s *Store) ListEpisodeKeys(ctx context.Context, afterKey string, limit, kind int) ([]string, error) {
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	query := `SELECT object_key FROM episodes
		 WHERE id > COALESCE((SELECT id FROM episodes WHERE object_key = ?), 0)`
	args := []any{afterKey}
	if kind >= 0 {
		// 游标子查询独立于本过滤条件：即使 afterKey 属于另一类别，仍能正确定位推进点
		query += ` AND kind = ?`
		args = append(args, kind)
	}
	query += ` ORDER BY id ASC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list episode keys: %w", err)
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, fmt.Errorf("scan episode key: %w", err)
		}
		keys = append(keys, k)
	}
	return keys, rows.Err()
}

func (s *Store) InsertEpisode(ctx context.Context, e Episode) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO episodes (worker_id, task_id, network_sha, game_count, total_steps, winner, object_key, created_at, kind)
		VALUES (?,?,?,?,?,?,?,?,?)`,
		e.WorkerID, e.TaskID, e.NetworkSha, e.GameCount, e.TotalSteps, e.Winner, e.ObjectKey, time.Now().Unix(), e.Kind)
	if err != nil {
		return fmt.Errorf("insert episode worker=%s task=%s: %w", e.WorkerID, e.TaskID, err)
	}
	return nil
}

// GetEpisodeByTask 取指定 task_id 已登记的 episode 元数据（ReportEpisode 幂等闸口）。
// 不存在时返回 (nil, nil)；多条并存（理论上 UNIQUE(task_id) 不允许）取最新一条。
//
// task_id 重复通常意味着 worker 在 R2 PUT 失败/超时后重试 ReportEpisode，此时应当
// 返回首次登记的对象键与一份新签名的 PUT URL——worker 可以原对象键继续上传，无需
// 重新分配 R2 路径。
func (s *Store) GetEpisodeByTask(ctx context.Context, taskID string) (*Episode, error) {
	var e Episode
	var createdAt int64
	err := s.db.QueryRowContext(ctx, `SELECT id, worker_id, task_id, network_sha, game_count, total_steps, winner, object_key, created_at, kind
		FROM episodes WHERE task_id = ? ORDER BY id DESC LIMIT 1`, taskID).
		Scan(&e.ID, &e.WorkerID, &e.TaskID, &e.NetworkSha, &e.GameCount,
			&e.TotalSteps, &e.Winner, &e.ObjectKey, &createdAt, &e.Kind)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get episode by task %s: %w", taskID, err)
	}
	e.CreatedAt = time.Unix(createdAt, 0)
	return &e, nil
}

// ListEpisodes 按 id 倒序分页（beforeID<=0 表示取最新一页）。
func (s *Store) ListEpisodes(ctx context.Context, beforeID int64, limit int) ([]Episode, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	query := `SELECT id, worker_id, task_id, network_sha, game_count, total_steps, winner, object_key, created_at, kind FROM episodes`
	args := []any{}
	if beforeID > 0 {
		query += ` WHERE id < ?`
		args = append(args, beforeID)
	}
	query += ` ORDER BY id DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list episodes: %w", err)
	}
	defer rows.Close()
	var out []Episode
	for rows.Next() {
		var e Episode
		var createdAt int64
		if err := rows.Scan(&e.ID, &e.WorkerID, &e.TaskID, &e.NetworkSha, &e.GameCount,
			&e.TotalSteps, &e.Winner, &e.ObjectKey, &createdAt, &e.Kind); err != nil {
			return nil, fmt.Errorf("scan episode row: %w", err)
		}
		e.CreatedAt = time.Unix(createdAt, 0)
		out = append(out, e)
	}
	return out, rows.Err()
}
