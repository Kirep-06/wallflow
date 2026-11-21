package repository_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Kirep-06/gocache"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"wallpaper-feed/internal/cache"
	"wallpaper-feed/internal/config"
	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/repository"
	"wallpaper-feed/internal/service"
)

// mysqlFollowMemDB 是一个遵循 MySQL 语义的轻量级内存 SQL 执行器（实现 database/sql/driver），
// 重点严格还原 MySQL 单表 UPDATE SET 子句“从左向右依次求值并立即写回当前行”的真实语义。
type mysqlFollowMemDB struct {
	mu          sync.Mutex
	nextEdgeID  uint64
	edges       map[[2]uint64]uint64
	stats       map[uint64]*followStatsRow
	executedSQL []string
}

type followStatsRow struct {
	userID         uint64
	followingCount int64
	followerCount  int64
	isBigV         int64
	updatedAt      time.Time
}

func newMySQLFollowMemDB() *mysqlFollowMemDB {
	return &mysqlFollowMemDB{
		nextEdgeID: 1,
		edges:      make(map[[2]uint64]uint64),
		stats:      make(map[uint64]*followStatsRow),
	}
}

func (db *mysqlFollowMemDB) seedStats(userID uint64, following, follower int64, isBigV int64) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.stats[userID] = &followStatsRow{
		userID:         userID,
		followingCount: following,
		followerCount:  follower,
		isBigV:         isBigV,
		updatedAt:      time.Now(),
	}
}

func (db *mysqlFollowMemDB) getStats(userID uint64) (followStatsRow, bool) {
	db.mu.Lock()
	defer db.mu.Unlock()
	row, ok := db.stats[userID]
	if !ok {
		return followStatsRow{}, false
	}
	return *row, true
}

func (db *mysqlFollowMemDB) openGorm(t *testing.T) *gorm.DB {
	t.Helper()
	sqlDB := sql.OpenDB(&mysqlFollowConnector{db: db})
	t.Cleanup(func() { _ = sqlDB.Close() })

	gdb, err := gorm.Open(mysql.New(mysql.Config{
		Conn:                      sqlDB,
		SkipInitializeWithVersion: true,
	}), &gorm.Config{
		Logger: logger.Discard,
	})
	if err != nil {
		t.Fatalf("open gorm with mysqlFollowMemDB: %v", err)
	}
	return gdb
}

type mysqlFollowConnector struct{ db *mysqlFollowMemDB }

func (c *mysqlFollowConnector) Connect(context.Context) (driver.Conn, error) {
	return &mysqlFollowConn{db: c.db}, nil
}

func (c *mysqlFollowConnector) Driver() driver.Driver { return &mysqlFollowDriver{db: c.db} }

type mysqlFollowDriver struct{ db *mysqlFollowMemDB }

func (d *mysqlFollowDriver) Open(string) (driver.Conn, error) {
	return &mysqlFollowConn{db: d.db}, nil
}

type mysqlFollowConn struct{ db *mysqlFollowMemDB }

func (c *mysqlFollowConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare not implemented")
}
func (c *mysqlFollowConn) Close() error { return nil }
func (c *mysqlFollowConn) Begin() (driver.Tx, error) {
	return &mysqlFollowTx{}, nil
}
func (c *mysqlFollowConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return &mysqlFollowTx{}, nil
}

type mysqlFollowTx struct{}

func (*mysqlFollowTx) Commit() error   { return nil }
func (*mysqlFollowTx) Rollback() error { return nil }

func (c *mysqlFollowConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.db.exec(query, namedValuesToAny(args))
}

func (c *mysqlFollowConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.db.query(query, namedValuesToAny(args))
}

func namedValuesToAny(args []driver.NamedValue) []any {
	out := make([]any, len(args))
	for i, a := range args {
		out[i] = a.Value
	}
	return out
}

var (
	reUpdateStats = regexp.MustCompile(`(?is)^\s*UPDATE\s+` + "`?" + `user_follow_stats` + "`?" + `\s+SET\s+(.+)\s+WHERE\s+` + "`?" + `user_id` + "`?" + `\s*=\s*\?\s*$`)
	reSelectLock  = regexp.MustCompile(`(?is)^\s*SELECT\s+.+\s+FROM\s+` + "`?" + `user_follow_stats` + "`?" + `\s+WHERE\s+` + "`?" + `user_id` + "`?" + `\s+IN\s*\(\s*\?\s*,\s*\?\s*\)`)
	reSelectPluck = regexp.MustCompile(`(?is)^\s*SELECT\s+` + "`?" + `user_id` + "`?" + `\s+FROM\s+` + "`?" + `user_follow_stats` + "`?" + `\s+WHERE\s+` + "`?" + `user_id` + "`?" + `\s+IN\s*\(\s*\?\s*(?:,\s*\?\s*)*\)`)
	reSelectEdge  = regexp.MustCompile(`(?is)^\s*SELECT\s+1\s+FROM\s+` + "`?" + `user_follows` + "`?" + `\s+WHERE\s+follower_id\s*=\s*\?\s+AND\s+followee_id\s*=\s*\?`)
	reInsertStats = regexp.MustCompile(`(?is)^\s*INSERT\s+IGNORE\s+INTO\s+` + "`?" + `user_follow_stats` + "`?")
	reInsertEdge  = regexp.MustCompile(`(?is)^\s*INSERT\s+IGNORE\s+INTO\s+` + "`?" + `user_follows` + "`?")
	reDeleteEdge  = regexp.MustCompile(`(?is)^\s*DELETE\s+FROM\s+` + "`?" + `user_follows` + "`?" + `\s+WHERE\s+follower_id\s*=\s*\?\s+AND\s+followee_id\s*=\s*\?`)
)

func (db *mysqlFollowMemDB) exec(query string, args []any) (driver.Result, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.executedSQL = append(db.executedSQL, query)

	switch {
	case reInsertStats.MatchString(query):
		uid := toUint64(args[0])
		if _, exists := db.stats[uid]; exists {
			return driver.RowsAffected(0), nil
		}
		db.stats[uid] = &followStatsRow{userID: uid, updatedAt: toTime(args[1])}
		return driver.RowsAffected(1), nil

	case reInsertEdge.MatchString(query):
		followerID, followeeID := toUint64(args[0]), toUint64(args[1])
		key := [2]uint64{followerID, followeeID}
		if _, exists := db.edges[key]; exists {
			return driver.RowsAffected(0), nil
		}
		db.edges[key] = db.nextEdgeID
		db.nextEdgeID++
		return driver.RowsAffected(1), nil

	case reDeleteEdge.MatchString(query):
		followerID, followeeID := toUint64(args[0]), toUint64(args[1])
		key := [2]uint64{followerID, followeeID}
		if _, exists := db.edges[key]; !exists {
			return driver.RowsAffected(0), nil
		}
		delete(db.edges, key)
		return driver.RowsAffected(1), nil

	case reUpdateStats.MatchString(query):
		m := reUpdateStats.FindStringSubmatch(query)
		setClause := m[1]
		if len(args) == 0 {
			return nil, fmt.Errorf("missing WHERE user_id arg for %q", query)
		}
		uid := toUint64(args[len(args)-1])
		row, ok := db.stats[uid]
		if !ok {
			return driver.RowsAffected(0), nil
		}
		if err := evalMySQLSetLeftToRight(row, setClause, args[:len(args)-1]); err != nil {
			return nil, fmt.Errorf("eval UPDATE %q: %w", query, err)
		}
		return driver.RowsAffected(1), nil

	default:
		return nil, fmt.Errorf("unsupported Exec SQL in test driver: %s", query)
	}
}

func (db *mysqlFollowMemDB) query(query string, args []any) (driver.Rows, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.executedSQL = append(db.executedSQL, query)

	switch {
	case reSelectPluck.MatchString(query):
		var rows [][]driver.Value
		for _, a := range args {
			uid := toUint64(a)
			if _, ok := db.stats[uid]; ok {
				rows = append(rows, []driver.Value{int64(uid)})
			}
		}
		return &memRows{cols: []string{"user_id"}, rows: rows}, nil

	case reSelectLock.MatchString(query):
		minID, maxID := toUint64(args[0]), toUint64(args[1])
		var rows [][]driver.Value
		for _, uid := range []uint64{minID, maxID} {
			if r, ok := db.stats[uid]; ok {
				rows = append(rows, []driver.Value{
					int64(r.userID),
					r.followingCount,
					r.followerCount,
					r.isBigV,
				})
			}
		}
		return &memRows{
			cols: []string{"user_id", "following_count", "follower_count", "is_bigv"},
			rows: rows,
		}, nil

	case reSelectEdge.MatchString(query):
		followerID, followeeID := toUint64(args[0]), toUint64(args[1])
		var rows [][]driver.Value
		if _, ok := db.edges[[2]uint64{followerID, followeeID}]; ok {
			rows = append(rows, []driver.Value{int64(1)})
		}
		return &memRows{cols: []string{"1"}, rows: rows}, nil

	default:
		return nil, fmt.Errorf("unsupported Query SQL in test driver: %s", query)
	}
}

// evalMySQLSetLeftToRight 解析并按 MySQL 单表 UPDATE 语义从左向右求值 SET 列表：
// 每一项赋值的结果立即写回 row，右侧表达式会观察到左侧已更新后的列值。
func evalMySQLSetLeftToRight(row *followStatsRow, setClause string, args []any) error {
	assignments := splitTopLevelComma(setClause)
	argIdx := 0
	for _, assign := range assignments {
		eqIdx := strings.IndexByte(assign, '=')
		if eqIdx <= 0 {
			return fmt.Errorf("invalid assignment %q", assign)
		}
		col := strings.ToLower(strings.Trim(strings.TrimSpace(assign[:eqIdx]), "`"))
		expr := strings.TrimSpace(assign[eqIdx+1:])

		switch col {
		case "updated_at":
			if expr != "?" {
				return fmt.Errorf("unexpected updated_at expr %q", expr)
			}
			if argIdx >= len(args) {
				return fmt.Errorf("missing arg for updated_at")
			}
			row.updatedAt = toTime(args[argIdx])
			argIdx++
		case "following_count", "follower_count", "is_bigv":
			val, err := evalIntExpr(row, expr, args, &argIdx)
			if err != nil {
				return fmt.Errorf("column %s expr %q: %w", col, expr, err)
			}
			// 立即写回当前行，确保后续赋值读到最新列值（MySQL 单表 UPDATE 从左向右赋值语义）。
			switch col {
			case "following_count":
				if val < 0 {
					return fmt.Errorf("mysql ERROR 1690 unsigned underflow on following_count: %d", val)
				}
				row.followingCount = val
			case "follower_count":
				if val < 0 {
					return fmt.Errorf("mysql ERROR 1690 unsigned underflow on follower_count: %d", val)
				}
				row.followerCount = val
			case "is_bigv":
				row.isBigV = val
			}
		default:
			return fmt.Errorf("unsupported column %q", col)
		}
	}
	if argIdx != len(args) {
		return fmt.Errorf("unconsumed args: used %d of %d", argIdx, len(args))
	}
	return nil
}

func evalIntExpr(row *followStatsRow, expr string, args []any, argIdx *int) (int64, error) {
	expr = strings.TrimSpace(expr)
	upper := strings.ToUpper(expr)

	// IF(cond, thenExpr, elseExpr)
	if strings.HasPrefix(upper, "IF(") && strings.HasSuffix(expr, ")") {
		inner := expr[3 : len(expr)-1]
		parts := splitTopLevelComma(inner)
		if len(parts) != 3 {
			return 0, fmt.Errorf("invalid IF() args: %q", expr)
		}
		cond, err := evalCondExpr(row, parts[0], args, argIdx)
		if err != nil {
			return 0, err
		}
		// 即使分支未选中也按占位符顺序消费参数（本测试中两分支无 ?，仍保持顺序严谨）
		thenVal, err := evalIntExpr(row, parts[1], args, argIdx)
		if err != nil {
			return 0, err
		}
		elseVal, err := evalIntExpr(row, parts[2], args, argIdx)
		if err != nil {
			return 0, err
		}
		if cond {
			return thenVal, nil
		}
		return elseVal, nil
	}

	// CASE WHEN cond THEN thenExpr ELSE elseExpr END
	if strings.HasPrefix(upper, "CASE ") && strings.HasSuffix(upper, " END") {
		reCase := regexp.MustCompile(`(?is)^CASE\s+WHEN\s+(.+?)\s+THEN\s+(.+?)\s+ELSE\s+(.+?)\s+END$`)
		m := reCase.FindStringSubmatch(expr)
		if len(m) != 4 {
			return 0, fmt.Errorf("invalid CASE expr: %q", expr)
		}
		cond, err := evalCondExpr(row, m[1], args, argIdx)
		if err != nil {
			return 0, err
		}
		thenVal, err := evalIntExpr(row, m[2], args, argIdx)
		if err != nil {
			return 0, err
		}
		elseVal, err := evalIntExpr(row, m[3], args, argIdx)
		if err != nil {
			return 0, err
		}
		if cond {
			return thenVal, nil
		}
		return elseVal, nil
	}

	// 二元加减：a + b / a - b
	if idx := findTopLevelAddSub(expr); idx > 0 {
		lhs, err := evalIntExpr(row, expr[:idx], args, argIdx)
		if err != nil {
			return 0, err
		}
		rhs, err := evalIntExpr(row, expr[idx+1:], args, argIdx)
		if err != nil {
			return 0, err
		}
		if expr[idx] == '+' {
			return lhs + rhs, nil
		}
		return lhs - rhs, nil
	}

	if expr == "?" {
		if *argIdx >= len(args) {
			return 0, errors.New("missing placeholder arg")
		}
		v := toInt64(args[*argIdx])
		*argIdx++
		return v, nil
	}
	if n, err := strconv.ParseInt(expr, 10, 64); err == nil {
		return n, nil
	}
	switch strings.ToLower(strings.Trim(expr, "`")) {
	case "following_count":
		return row.followingCount, nil
	case "follower_count":
		return row.followerCount, nil
	case "is_bigv":
		return row.isBigV, nil
	default:
		return 0, fmt.Errorf("unknown identifier/expr %q", expr)
	}
}

func evalCondExpr(row *followStatsRow, cond string, args []any, argIdx *int) (bool, error) {
	cond = strings.TrimSpace(cond)
	for _, op := range []string{">=", "<=", "!=", "=", ">", "<"} {
		if idx := strings.Index(cond, op); idx > 0 {
			lhs, err := evalIntExpr(row, cond[:idx], args, argIdx)
			if err != nil {
				return false, err
			}
			rhs, err := evalIntExpr(row, cond[idx+len(op):], args, argIdx)
			if err != nil {
				return false, err
			}
			switch op {
			case ">=":
				return lhs >= rhs, nil
			case "<=":
				return lhs <= rhs, nil
			case "!=":
				return lhs != rhs, nil
			case "=":
				return lhs == rhs, nil
			case ">":
				return lhs > rhs, nil
			case "<":
				return lhs < rhs, nil
			}
		}
	}
	return false, fmt.Errorf("unsupported condition %q", cond)
}

func splitTopLevelComma(s string) []string {
	var parts []string
	depth := 0
	start := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, strings.TrimSpace(s[start:i]))
				start = i + 1
			}
		}
	}
	if tail := strings.TrimSpace(s[start:]); tail != "" {
		parts = append(parts, tail)
	}
	return parts
}

func findTopLevelAddSub(s string) int {
	depth := 0
	for i := len(s) - 1; i > 0; i-- {
		switch s[i] {
		case ')':
			depth++
		case '(':
			depth--
		case '+', '-':
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

type memRows struct {
	cols []string
	rows [][]driver.Value
	idx  int
}

func (r *memRows) Columns() []string { return r.cols }
func (r *memRows) Close() error      { return nil }
func (r *memRows) Next(dest []driver.Value) error {
	if r.idx >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.idx])
	r.idx++
	return nil
}

func toUint64(v any) uint64 {
	switch n := v.(type) {
	case int64:
		return uint64(n)
	case uint64:
		return n
	case int:
		return uint64(n)
	case int32:
		return uint64(n)
	case uint32:
		return uint64(n)
	default:
		panic(fmt.Sprintf("unexpected uint64 arg type %T (%v)", v, v))
	}
}

func toInt64(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case uint64:
		return int64(n)
	case int:
		return int64(n)
	case int32:
		return int64(n)
	case uint32:
		return int64(n)
	default:
		panic(fmt.Sprintf("unexpected int64 arg type %T (%v)", v, v))
	}
}

func toTime(v any) time.Time {
	if t, ok := v.(time.Time); ok {
		return t
	}
	return time.Now()
}

type stubUserExistsReader struct{}

func (stubUserExistsReader) Exists(context.Context, uint64) (bool, error) { return true, nil }
func (stubUserExistsReader) GetPublicProfiles(context.Context, []uint64) (map[uint64]*model.UserProfileDTO, error) {
	return map[uint64]*model.UserProfileDTO{}, nil
}

type hookEvent struct {
	followerID     uint64
	followeeID     uint64
	followeeIsBigV bool
	promotedBigV   bool
}

type spyFollowHook struct {
	events []hookEvent
}

func (h *spyFollowHook) OnFollowChanged(_ context.Context, followerID, followeeID uint64, followeeIsBigV, promotedBigV bool) {
	h.events = append(h.events, hookEvent{
		followerID:     followerID,
		followeeID:     followeeID,
		followeeIsBigV: followeeIsBigV,
		promotedBigV:   promotedBigV,
	})
}

// TestMySQLUpdateLeftToRightSanityCheck 验证测试执行器确实严格按 MySQL 从左向右求值：
// 对修复前的旧 SQL，在 4998 -> 4999 时会因双加一提前把 is_bigv 置为 1。
func TestMySQLUpdateLeftToRightSanityCheck(t *testing.T) {
	row := &followStatsRow{userID: 100, followerCount: 4998, isBigV: 0}
	oldBuggySet := "follower_count = follower_count + 1, is_bigv = IF(follower_count + 1 >= ?, 1, is_bigv), updated_at = ?"
	if err := evalMySQLSetLeftToRight(row, oldBuggySet, []any{int64(5000), time.Now()}); err != nil {
		t.Fatalf("eval old SQL failed: %v", err)
	}
	if row.followerCount != 4999 || row.isBigV != 1 {
		t.Fatalf("expected MySQL left-to-right evaluator to reproduce premature promotion (4999, is_bigv=1), got count=%d is_bigv=%d",
			row.followerCount, row.isBigV)
	}
}

// TestFollowRepo_BigVPromotionSQLAndHookConsistency 端到端验证 FollowService -> FollowRepo 实际 SQL -> Hook 状态完全一致：
// - 4998 -> 4999：不晋升；
// - 重复关注：不重复计数，不触发 Hook；
// - 4999 -> 5000：首次晋升，DB 与 Hook 均标记为大 V 且 promotedBigV=true；
// - 晋升后重复关注：不重复计数，不触发 Hook；
// - 晋升后掉粉（5000 -> 4999）：计数扣减，is_bigv 保持 1 不降级；
// - 掉粉后再涨回 5000：保持 is_bigv=1，且 promotedBigV=false（非首次晋升）。
func TestFollowRepo_BigVPromotionSQLAndHookConsistency(t *testing.T) {
	ctx := context.Background()
	memDB := newMySQLFollowMemDB()
	const (
		authorID    uint64 = 1000
		fan4999     uint64 = 2001
		fan5000     uint64 = 2002
		fanReFollow uint64 = 2003
		threshold          = 5000
	)
	memDB.seedStats(authorID, 0, 4998, 0)

	gdb := memDB.openGorm(t)
	followRepo := repository.NewFollowRepo(gdb)

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	statsCache := cache.NewMultiLevel(gocache.NewCache(1<<20), rdb, cache.Options{
		Prefix:   "follow:stats:",
		LocalTTL: 15 * time.Second,
		NullTTL:  60 * time.Second,
	})
	followSvc := service.NewFollowService(followRepo, stubUserExistsReader{}, statsCache, config.FollowConfig{
		MaxFollowing:  2000,
		StatsLocalTTL: 15,
		StatsRedisTTL: 600,
	})
	hook := &spyFollowHook{}
	followSvc.SetFollowingFeedHook(hook, threshold)

	assertAuthorStats := func(wantCount, wantBigV int64) {
		t.Helper()
		st, ok := memDB.getStats(authorID)
		if !ok {
			t.Fatalf("author %d stats row missing", authorID)
		}
		if st.followerCount != wantCount || st.isBigV != wantBigV {
			t.Fatalf("author DB stats mismatch: got follower_count=%d is_bigv=%d, want follower_count=%d is_bigv=%d",
				st.followerCount, st.isBigV, wantCount, wantBigV)
		}
	}

	// 1. 4998 -> 4999：不晋升，DB 与 Hook 均为非大 V。
	if err := followSvc.Follow(ctx, fan4999, authorID); err != nil {
		t.Fatalf("follow 4998->4999: %v", err)
	}
	assertAuthorStats(4999, 0)
	if len(hook.events) != 1 || hook.events[0] != (hookEvent{fan4999, authorID, false, false}) {
		t.Fatalf("unexpected hook events after 4998->4999: %+v", hook.events)
	}

	// 2. 重复关注：不重复计数、不晋升、不重复触发 Hook。
	if err := followSvc.Follow(ctx, fan4999, authorID); err != nil {
		t.Fatalf("duplicate follow at 4999: %v", err)
	}
	assertAuthorStats(4999, 0)
	if len(hook.events) != 1 {
		t.Fatalf("duplicate follow must not fire hook, got %+v", hook.events)
	}

	// 3. 4999 -> 5000：首次晋升，DB is_bigv=1 且 Hook 收到 followeeIsBigV=true, promotedBigV=true。
	if err := followSvc.Follow(ctx, fan5000, authorID); err != nil {
		t.Fatalf("follow 4999->5000: %v", err)
	}
	assertAuthorStats(5000, 1)
	if len(hook.events) != 2 || hook.events[1] != (hookEvent{fan5000, authorID, true, true}) {
		t.Fatalf("unexpected hook events after 4999->5000 promotion: %+v", hook.events)
	}

	// 4. 晋升后重复关注：不重复计数、不触发 Hook。
	if err := followSvc.Follow(ctx, fan5000, authorID); err != nil {
		t.Fatalf("duplicate follow at 5000: %v", err)
	}
	assertAuthorStats(5000, 1)
	if len(hook.events) != 2 {
		t.Fatalf("duplicate follow after promotion must not fire hook, got %+v", hook.events)
	}

	// 5. 晋升后掉粉（5000 -> 4999）：计数扣减，但 is_bigv 保持 1 不降级。
	if err := followSvc.Unfollow(ctx, fan5000, authorID); err != nil {
		t.Fatalf("unfollow 5000->4999: %v", err)
	}
	assertAuthorStats(4999, 1)
	if len(hook.events) != 3 || hook.events[2] != (hookEvent{fan5000, authorID, true, false}) {
		t.Fatalf("unexpected hook events after unfollow 5000->4999: %+v", hook.events)
	}

	// 6. 掉粉后再涨粉（4999 -> 5000）：保持 is_bigv=1，且 promotedBigV=false（非首次晋升）。
	if err := followSvc.Follow(ctx, fanReFollow, authorID); err != nil {
		t.Fatalf("follow 4999->5000 after already bigv: %v", err)
	}
	assertAuthorStats(5000, 1)
	if len(hook.events) != 4 || hook.events[3] != (hookEvent{fanReFollow, authorID, true, false}) {
		t.Fatalf("unexpected hook events after re-reaching 5000: %+v", hook.events)
	}
}

// TestFollowRepo_BigVPromotionRealMySQL 当显式设置 TEST_MYSQL_DSN 时，直接在真实 MySQL 8 实例上执行同一套验证。
func TestFollowRepo_BigVPromotionRealMySQL(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("TEST_MYSQL_DSN"))
	if dsn == "" {
		t.Skip("TEST_MYSQL_DSN not set; skipping real MySQL integration check")
	}
	ctx := context.Background()
	gdb, err := gorm.Open(mysql.Open(dsn), &gorm.Config{Logger: logger.Discard})
	if err != nil {
		t.Fatalf("connect real mysql: %v", err)
	}
	sqlDB, err := gdb.DB()
	if err != nil {
		t.Fatalf("sql.DB: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })

	const (
		authorID uint64 = 980_000_001
		fan1     uint64 = 980_000_002
		fan2     uint64 = 980_000_003
		fan3     uint64 = 980_000_004
	)
	cleanup := func() {
		_ = gdb.Exec("DELETE FROM user_follows WHERE followee_id = ?", authorID).Error
		_ = gdb.Exec("DELETE FROM user_follow_stats WHERE user_id IN (?, ?, ?, ?)", authorID, fan1, fan2, fan3).Error
	}
	cleanup()
	t.Cleanup(cleanup)

	now := time.Now().Truncate(time.Millisecond)
	if err := gdb.Exec(
		"INSERT INTO user_follow_stats (user_id, following_count, follower_count, is_bigv, updated_at) VALUES (?, 0, 4998, 0, ?)",
		authorID, now,
	).Error; err != nil {
		t.Fatalf("seed real mysql author stats: %v", err)
	}

	repo := repository.NewFollowRepo(gdb)

	// 4998 -> 4999
	res, err := repo.Follow(ctx, fan1, authorID, 2000, 5000, now)
	if err != nil {
		t.Fatalf("Follow 4998->4999: %v", err)
	}
	st, err := repo.GetStats(ctx, authorID)
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	if !res.Changed || res.FolloweeIsBigV || res.PromotedBigV || st.FollowerCount != 4999 || st.IsBigV {
		t.Fatalf("4998->4999 mismatch: res=%+v db=%+v", res, st)
	}

	// 重复关注
	dup, err := repo.Follow(ctx, fan1, authorID, 2000, 5000, now)
	if err != nil || dup.Changed {
		t.Fatalf("duplicate follow mismatch: res=%+v err=%v", dup, err)
	}
	if st, _ = repo.GetStats(ctx, authorID); st.FollowerCount != 4999 || st.IsBigV {
		t.Fatalf("duplicate follow changed DB: %+v", st)
	}

	// 4999 -> 5000
	res, err = repo.Follow(ctx, fan2, authorID, 2000, 5000, now)
	if err != nil {
		t.Fatalf("Follow 4999->5000: %v", err)
	}
	st, _ = repo.GetStats(ctx, authorID)
	if !res.Changed || !res.FolloweeIsBigV || !res.PromotedBigV || st.FollowerCount != 5000 || !st.IsBigV {
		t.Fatalf("4999->5000 mismatch: res=%+v db=%+v", res, st)
	}

	// 5000 -> 4999 不降级
	un, err := repo.Unfollow(ctx, fan2, authorID, now)
	if err != nil {
		t.Fatalf("Unfollow 5000->4999: %v", err)
	}
	st, _ = repo.GetStats(ctx, authorID)
	if !un.Changed || !un.FolloweeIsBigV || un.PromotedBigV || st.FollowerCount != 4999 || !st.IsBigV {
		t.Fatalf("Unfollow mismatch: res=%+v db=%+v", un, st)
	}

	// 4999 -> 5000 非首次晋升
	res, err = repo.Follow(ctx, fan3, authorID, 2000, 5000, now)
	if err != nil {
		t.Fatalf("Follow re-5000: %v", err)
	}
	st, _ = repo.GetStats(ctx, authorID)
	if !res.Changed || !res.FolloweeIsBigV || res.PromotedBigV || st.FollowerCount != 5000 || !st.IsBigV {
		t.Fatalf("re-5000 mismatch: res=%+v db=%+v", res, st)
	}
}
