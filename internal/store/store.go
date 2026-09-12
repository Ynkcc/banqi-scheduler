package store

import (
	"database/sql"
	"fmt"
	"strings"
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
	CreatedAt  time.Time
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
}

// ListEpisodeKeys 游标分页列出已登记的 episode 对象键（字典序递增）。
// afterKey 为上次返回的最后一个键；limit<=0 时取默认 200。
func (s *Store) ListEpisodeKeys(afterKey string, limit int) ([]string, error) {
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	rows, err := s.db.Query(
		`SELECT object_key FROM episodes WHERE object_key > ? ORDER BY object_key ASC LIMIT ?`,
		afterKey, limit)
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

type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)")
	if err != nil {
		return nil, fmt.Errorf("open sqlite %s: %w", path, err)
	}
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate() error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS networks (
			sha TEXT PRIMARY KEY,
			parent_sha TEXT,
			created_at INTEGER NOT NULL,
			is_best INTEGER NOT NULL DEFAULT 0,
			status TEXT NOT NULL DEFAULT 'candidate',
			notes TEXT
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
		`CREATE TABLE IF NOT EXISTS episodes (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			worker_id TEXT NOT NULL,
			task_id TEXT NOT NULL,
			network_sha TEXT NOT NULL,
			game_count INTEGER NOT NULL,
			total_steps INTEGER NOT NULL,
			winner INTEGER NOT NULL,
			object_key TEXT NOT NULL,
			created_at INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_episodes_network ON episodes(network_sha)`,
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
		// 旧库升级：workers 表补列（已存在时忽略错误）
		`ALTER TABLE workers ADD COLUMN client_version TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE workers ADD COLUMN memory_mb INTEGER NOT NULL DEFAULT 0`,
	}
	for _, q := range stmts {
		if _, err := s.db.Exec(q); err != nil {
			// 容忍旧库升级时列已存在
			if strings.Contains(err.Error(), "duplicate column name") {
				continue
			}
			return fmt.Errorf("exec %q: %w", q[:40], err)
		}
	}
	return nil
}

const networkColumns = `sha, COALESCE(parent_sha,''), created_at, is_best, status, COALESCE(notes,'')`

// scanNetworkRow 统一 networks 行扫描，*sql.Row 与 *sql.Rows 通用。
func scanNetworkRow(row interface{ Scan(...any) error }) (*Network, error) {
	n := &Network{}
	var createdAt int64
	if err := row.Scan(&n.Sha, &n.ParentSha, &createdAt, &n.IsBest, &n.Status, &n.Notes); err != nil {
		return nil, err
	}
	n.CreatedAt = time.Unix(createdAt, 0)
	return n, nil
}

func (s *Store) GetBest() (*Network, error) {
	n, err := scanNetworkRow(s.db.QueryRow(`SELECT ` + networkColumns + ` FROM networks WHERE is_best=1 LIMIT 1`))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get best network: %w", err)
	}
	return n, nil
}

func (s *Store) GetNetwork(sha string) (*Network, error) {
	n, err := scanNetworkRow(s.db.QueryRow(`SELECT `+networkColumns+` FROM networks WHERE sha=?`, sha))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get network %s: %w", sha, err)
	}
	return n, nil
}

func (s *Store) RegisterNetwork(sha, parentSha, notes string) (created bool, err error) {
	now := time.Now().Unix()
	res, err := s.db.Exec(`INSERT OR IGNORE INTO networks (sha, parent_sha, created_at, status, notes) VALUES (?,?,?,'candidate',?)`,
		sha, parentSha, now, notes)
	if err != nil {
		return false, fmt.Errorf("insert network %s: %w", sha, err)
	}
	aff, _ := res.RowsAffected()
	return aff > 0, nil
}

// PromoteBest 原子切换 best 指针并标记 candidate 晋级
func (s *Store) PromoteBest(sha string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("promote begin: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`UPDATE networks SET is_best=0, status='archived' WHERE is_best=1`); err != nil {
		return fmt.Errorf("demote old best: %w", err)
	}
	if _, err := tx.Exec(`UPDATE networks SET is_best=1, status='best' WHERE sha=?`, sha); err != nil {
		return fmt.Errorf("promote %s: %w", sha, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("promote commit: %w", err)
	}
	return nil
}

func (s *Store) UpdateNetworkStatus(sha, status string) error {
	_, err := s.db.Exec(`UPDATE networks SET status=? WHERE sha=?`, status, sha)
	if err != nil {
		return fmt.Errorf("update network %s status=%s: %w", sha, status, err)
	}
	return nil
}

func (s *Store) CreateMatch(candidate, opponent string, targetGames int) (int64, error) {
	res, err := s.db.Exec(`INSERT INTO matches (candidate, opponent, target_games) VALUES (?,?,?)`,
		candidate, opponent, targetGames)
	if err != nil {
		return 0, fmt.Errorf("create match %s vs %s: %w", candidate, opponent, err)
	}
	return res.LastInsertId()
}

// PendingMatch 取最早一个未完结的 gatekeeper 对打
func (s *Store) PendingMatch() (*Match, error) {
	return s.scanMatch(`SELECT id, candidate, opponent, status, pair_ll, pair_ld, pair_dd, pair_dw, pair_ww, num_games, target_games
		FROM matches WHERE status='running' ORDER BY id LIMIT 1`, nil)
}

func (s *Store) GetMatch(id int64) (*Match, error) {
	return s.scanMatch(`SELECT id, candidate, opponent, status, pair_ll, pair_ld, pair_dd, pair_dw, pair_ww, num_games, target_games
		FROM matches WHERE id=?`, id)
}

func (s *Store) scanMatch(query string, arg any) (*Match, error) {
	m := &Match{}
	err := s.db.QueryRow(query, arg).
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

func (s *Store) UpdateMatchResult(id int64, pairs [5]int, numGames int, status string) error {
	_, err := s.db.Exec(`UPDATE matches SET pair_ll=?, pair_ld=?, pair_dd=?, pair_dw=?, pair_ww=?, num_games=?, status=? WHERE id=?`,
		pairs[0], pairs[1], pairs[2], pairs[3], pairs[4], numGames, status, id)
	if err != nil {
		return fmt.Errorf("update match %d: %w", id, err)
	}
	return nil
}

func (s *Store) InsertEpisode(e Episode) error {
	_, err := s.db.Exec(`INSERT INTO episodes (worker_id, task_id, network_sha, game_count, total_steps, winner, object_key, created_at)
		VALUES (?,?,?,?,?,?,?,?)`,
		e.WorkerID, e.TaskID, e.NetworkSha, e.GameCount, e.TotalSteps, e.Winner, e.ObjectKey, time.Now().Unix())
	if err != nil {
		return fmt.Errorf("insert episode worker=%s task=%s: %w", e.WorkerID, e.TaskID, err)
	}
	return nil
}

func (s *Store) TouchWorker(id string, threads, completedGames int, clientVersion string, memoryMb int64) error {
	_, err := s.db.Exec(`INSERT INTO workers (id, last_seen, threads, completed_games, client_version, memory_mb) VALUES (?,?,?,?,?,?)
		ON CONFLICT(id) DO UPDATE SET last_seen=excluded.last_seen, threads=excluded.threads, completed_games=excluded.completed_games,
		client_version=excluded.client_version, memory_mb=excluded.memory_mb`,
		id, time.Now().Unix(), threads, completedGames, clientVersion, memoryMb)
	if err != nil {
		return fmt.Errorf("touch worker %s: %w", id, err)
	}
	return nil
}

// WorkerVersion 返回该 worker 最近一次上报的版本声明
func (s *Store) WorkerVersion(id string) (string, error) {
	var v string
	err := s.db.QueryRow(`SELECT client_version FROM workers WHERE id=?`, id).Scan(&v)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("worker version %s: %w", id, err)
	}
	return v, nil
}

// GetSetting 读取运行时设置；键不存在时返回空串。
func (s *Store) GetSetting(key string) (string, error) {
	var v string
	err := s.db.QueryRow(`SELECT value FROM settings WHERE key=?`, key).Scan(&v)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("get setting %s: %w", key, err)
	}
	return v, nil
}

func (s *Store) SetSetting(key, value string) error {
	_, err := s.db.Exec(`INSERT INTO settings (key, value) VALUES (?,?)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, value)
	if err != nil {
		return fmt.Errorf("set setting %s=%s: %w", key, value, err)
	}
	return nil
}

func (s *Store) ListNetworks() ([]Network, error) {
	rows, err := s.db.Query(`SELECT ` + networkColumns + ` FROM networks ORDER BY is_best DESC, created_at DESC`)
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

func (s *Store) ListMatches() ([]Match, error) {
	rows, err := s.db.Query(`SELECT id, candidate, opponent, status, pair_ll, pair_ld, pair_dd, pair_dw, pair_ww, num_games, target_games
		FROM matches ORDER BY CASE status WHEN 'running' THEN 0 ELSE 1 END, id DESC`)
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

func (s *Store) ListWorkers() ([]Worker, error) {
	rows, err := s.db.Query(`SELECT id, last_seen, threads, completed_games, client_version, memory_mb
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

// ListEpisodes 按 id 倒序分页（beforeID<=0 表示取最新一页）。
func (s *Store) ListEpisodes(beforeID int64, limit int) ([]Episode, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	query := `SELECT id, worker_id, task_id, network_sha, game_count, total_steps, winner, object_key, created_at FROM episodes`
	args := []any{}
	if beforeID > 0 {
		query += ` WHERE id < ?`
		args = append(args, beforeID)
	}
	query += ` ORDER BY id DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("list episodes: %w", err)
	}
	defer rows.Close()
	var out []Episode
	for rows.Next() {
		var e Episode
		var createdAt int64
		if err := rows.Scan(&e.ID, &e.WorkerID, &e.TaskID, &e.NetworkSha, &e.GameCount,
			&e.TotalSteps, &e.Winner, &e.ObjectKey, &createdAt); err != nil {
			return nil, fmt.Errorf("scan episode row: %w", err)
		}
		e.CreatedAt = time.Unix(createdAt, 0)
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) Counts() (Counts, error) {
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
	}
	for _, t := range targets {
		if err := s.db.QueryRow(t.query).Scan(t.dst); err != nil {
			return c, fmt.Errorf("counts %q: %w", t.query, err)
		}
	}
	if err := s.db.QueryRow(`SELECT COALESCE(SUM(game_count),0) FROM episodes`).Scan(&c.EpisodeGames); err != nil {
		return c, fmt.Errorf("counts episode_games: %w", err)
	}
	return c, nil
}
