// Package store 封装调度器的 SQLite 元数据（modernc 纯 Go 驱动，WAL）。
//
// 文件划分：
//   - store.go    连接生命周期与建表迁移 + 跨表统计（Counts）+ 运行时设置
//   - networks.go networks 表（best 指针 / 登记 / 状态）
//   - matches.go  matches 表（gatekeeper 对打与五项成对计数）
//   - eval.go     eval_results 表（绝对强度评估趋势，不参与晋级判定）
//   - episodes.go episodes 表（元数据登记与游标分页）
//   - workers.go  workers 表（心跳状态与版本）
//
// 所有方法以 context 为首参，便于随请求取消。
package store

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "modernc.org/sqlite"
)

type Network struct {
	Sha       string
	ParentSha string
	CreatedAt time.Time
	IsBest    bool
	Status    string // candidate | best | rejected
	Notes     string
	Format    string // 权重格式（onnx / pt / nnue）：决定 R2 对象键扩展名
}

type Match struct {
	ID          int64
	Candidate   string
	Opponent    string
	Status      string // running | concluded
	Pairs       [5]int // [LL, LD, DD, DW, WW]
	NumGames    int
	TargetGames int
}

type Episode struct {
	ID         int64
	WorkerID   string
	TaskID     string
	NetworkSha string
	GameCount  int
	TotalSteps int
	Winner     int
	ObjectKey  string
	// Kind 数据类别（pb.DataKind 的整数值：0=ResNet/MCTS，1=NNUE）。
	Kind      int
	CreatedAt time.Time
}

type Worker struct {
	ID             string
	LastSeen       time.Time
	Threads        int
	CompletedGames int
	ClientVersion  string
	MemoryMb       int64
}

type Counts struct {
	Networks       int `json:"networks"`
	Candidates     int `json:"candidates"`
	MatchesRunning int `json:"matchesRunning"`
	Episodes       int `json:"episodes"`
	EpisodeGames   int `json:"episodeGames"`
	Workers        int `json:"workers"`
	EvalResults    int `json:"evalResults"`
}

type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)")
	if err != nil {
		return nil, fmt.Errorf("open sqlite %s: %w", path, err)
	}
	s := &Store{db: db}
	if err := s.migrate(context.Background()); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS networks (
			sha TEXT PRIMARY KEY,
			parent_sha TEXT,
			created_at INTEGER NOT NULL,
			is_best INTEGER NOT NULL DEFAULT 0,
			status TEXT NOT NULL DEFAULT 'candidate',
			notes TEXT,
			format TEXT NOT NULL DEFAULT 'onnx'
		)`,
		`CREATE TABLE IF NOT EXISTS matches (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			candidate TEXT NOT NULL REFERENCES networks(sha),
			opponent TEXT NOT NULL REFERENCES networks(sha),
			status TEXT NOT NULL DEFAULT 'running',
			pair_ll INTEGER NOT NULL DEFAULT 0,
			pair_ld INTEGER NOT NULL DEFAULT 0,
			pair_dd INTEGER NOT NULL DEFAULT 0,
			pair_dw INTEGER NOT NULL DEFAULT 0,
			pair_ww INTEGER NOT NULL DEFAULT 0,
			num_games INTEGER NOT NULL DEFAULT 0,
			target_games INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS eval_results (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			network_sha TEXT NOT NULL,
			opponent_spec TEXT NOT NULL,
			wins INTEGER NOT NULL DEFAULT 0,
			draws INTEGER NOT NULL DEFAULT 0,
			losses INTEGER NOT NULL DEFAULT 0,
			num_games INTEGER NOT NULL DEFAULT 0,
			avg_moves REAL NOT NULL DEFAULT 0,
			created_at INTEGER NOT NULL,
			UNIQUE(network_sha, opponent_spec)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_eval_results_spec ON eval_results(opponent_spec, id)`,
		`CREATE TABLE IF NOT EXISTS episodes (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			worker_id TEXT NOT NULL,
			task_id TEXT NOT NULL,
			network_sha TEXT NOT NULL,
			game_count INTEGER NOT NULL,
			total_steps INTEGER NOT NULL,
			winner INTEGER NOT NULL,
			object_key TEXT NOT NULL,
			created_at INTEGER NOT NULL,
			kind INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE INDEX IF NOT EXISTS idx_episodes_network ON episodes(network_sha)`,
		`CREATE INDEX IF NOT EXISTS idx_episodes_object_key ON episodes(object_key)`,
		`CREATE TABLE IF NOT EXISTS workers (
			id TEXT PRIMARY KEY,
			last_seen INTEGER NOT NULL,
			threads INTEGER NOT NULL DEFAULT 0,
			completed_games INTEGER NOT NULL DEFAULT 0,
			client_version TEXT NOT NULL DEFAULT '',
			memory_mb INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS settings (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)`,
		// eval_queue：进程重启后必须能恢复的待办评测。
		// 主键 (network_sha, opponent_spec) 与 eval_results.UNIQUE 同语义——同 (best, 规则)
		// 同时只能有一行「待办」与一条「结果」，重叠由 GetEvalResult 的 UNIQUE 兜底。
		`CREATE TABLE IF NOT EXISTS eval_queue (
			network_sha TEXT NOT NULL,
			opponent_spec TEXT NOT NULL,
			attempts INTEGER NOT NULL DEFAULT 0,
			created_at INTEGER NOT NULL,
			PRIMARY KEY (network_sha, opponent_spec)
		)`,
	}
	for _, q := range stmts {
		if _, err := s.db.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("exec %q: %w", q, err)
		}
	}

	// 旧库升级：补列。按实际列存在性判断，不依赖驱动的错误文案。
	addColumn := []struct{ table, name, ddl string }{
		{"workers", "client_version", `ALTER TABLE workers ADD COLUMN client_version TEXT NOT NULL DEFAULT ''`},
		{"workers", "memory_mb", `ALTER TABLE workers ADD COLUMN memory_mb INTEGER NOT NULL DEFAULT 0`},
		{"networks", "format", `ALTER TABLE networks ADD COLUMN format TEXT NOT NULL DEFAULT 'onnx'`},
		{"episodes", "kind", `ALTER TABLE episodes ADD COLUMN kind INTEGER NOT NULL DEFAULT 0`},
	}
	for _, c := range addColumn {
		exists, err := s.columnExists(ctx, c.table, c.name)
		if err != nil {
			return err
		}
		if exists {
			continue
		}
		if _, err := s.db.ExecContext(ctx, c.ddl); err != nil {
			return fmt.Errorf("add %s.%s: %w", c.table, c.name, err)
		}
	}

	// 依赖补列的索引必须在补列之后创建：老库上先建索引会因「no such column」直接打不开。
	postIndexes := []string{
		`CREATE INDEX IF NOT EXISTS idx_episodes_kind ON episodes(kind)`,
	}
	for _, q := range postIndexes {
		if _, err := s.db.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("exec %q: %w", q, err)
		}
	}
	// 幂等保护：episodes.task_id 唯一约束，防止 ReportEpisode 重试产生重复行。
	// 用 CREATE UNIQUE INDEX 而非 ALTER TABLE ADD CONSTRAINT（SQLite 不支持后者）。
	// 现有数据已有重复 task_id 时建索引会失败——这是数据污染的信号，降级为应用层查重
	// （GetEpisodeByTask 仍工作），不阻断启动。
	if _, err := s.db.ExecContext(ctx, `CREATE UNIQUE INDEX IF NOT EXISTS idx_episodes_task_id_unique ON episodes(task_id)`); err != nil {
		log.Printf("[store] ⚠️ 创建 episodes.task_id 唯一索引失败（%v）；现有数据可能含重复 task_id，幂等保护降级为应用层查重", err)
	}
	return nil
}

// columnExists 查询表是否已含指定列（table-valued PRAGMA，SQLite 3.16+）。
func (s *Store) columnExists(ctx context.Context, table, column string) (bool, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT 1 FROM pragma_table_info(?) WHERE name = ? LIMIT 1`, table, column)
	if err != nil {
		return false, fmt.Errorf("pragma_table_info(%s): %w", table, err)
	}
	defer rows.Close()
	exists := rows.Next()
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("pragma_table_info(%s): %w", table, err)
	}
	return exists, nil
}

// Counts 汇总 WebUI 首页计数（跨表统计）。
func (s *Store) Counts(ctx context.Context) (Counts, error) {
	var c Counts
	targets := []struct {
		query string
		dst   *int
	}{
		{`SELECT COUNT(*) FROM networks`, &c.Networks},
		{`SELECT COUNT(*) FROM networks WHERE status='candidate'`, &c.Candidates},
		{`SELECT COUNT(*) FROM matches WHERE status='running'`, &c.MatchesRunning},
		{`SELECT COUNT(*) FROM episodes`, &c.Episodes},
		{`SELECT COUNT(*) FROM workers`, &c.Workers},
		{`SELECT COUNT(*) FROM eval_results`, &c.EvalResults},
	}
	for _, t := range targets {
		if err := s.db.QueryRowContext(ctx, t.query).Scan(t.dst); err != nil {
			return c, fmt.Errorf("counts %q: %w", t.query, err)
		}
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(SUM(game_count),0) FROM episodes`).Scan(&c.EpisodeGames); err != nil {
		return c, fmt.Errorf("counts episode_games: %w", err)
	}
	return c, nil
}

// GetSetting 读取运行时设置；键不存在时返回空串。
func (s *Store) GetSetting(ctx context.Context, key string) (string, error) {
	var v string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM settings WHERE key=?`, key).Scan(&v)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("get setting %s: %w", key, err)
	}
	return v, nil
}

func (s *Store) SetSetting(ctx context.Context, key, value string) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO settings (key, value) VALUES (?,?)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, value)
	if err != nil {
		return fmt.Errorf("set setting %s=%s: %w", key, value, err)
	}
	return nil
}

// IncSetting 原子地将键值视为整数自增；用于 eval_promotions 等单调计数器。
// 不存在的键从 0 起计。原子语义保证多 goroutine 并发自增不丢更新（SQLite WAL
// 单写者即可序列化）。
func (s *Store) IncSetting(ctx context.Context, key string) (int, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	var cur int
	err = tx.QueryRowContext(ctx, `SELECT COALESCE(
		(SELECT CAST(value AS INTEGER) FROM settings WHERE key = ?), 0)`, key).Scan(&cur)
	if err != nil {
		return 0, fmt.Errorf("read counter %s: %w", key, err)
	}
	cur++
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO settings (key, value) VALUES (?, ?)
		 ON CONFLICT(key) DO UPDATE SET value=excluded.value`,
		key, fmt.Sprintf("%d", cur)); err != nil {
		return 0, fmt.Errorf("write counter %s: %w", key, err)
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return cur, nil
}
