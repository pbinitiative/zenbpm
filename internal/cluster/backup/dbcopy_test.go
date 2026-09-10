package backup

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/pbinitiative/zenbpm/internal/cluster/zenerr"
	rqcmd "github.com/rqlite/rqlite/v10/command/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCopyDatabaseReproducesTheImage(t *testing.T) {
	ctx := context.Background()
	imagePath := buildImage(t)
	targetPath := filepath.Join(t.TempDir(), "target.db")
	// the target enforces foreign keys: the copy has to order tables and
	// batches so that every batch commits without dangling references
	target := &sqliteTarget{db: openSQLite(t, targetPath, true)}
	seedStaleTarget(t, target.db)

	// small batches: the copy has to split tables across many transactions
	report, err := CopyDatabase(ctx, imagePath, target, CopyOptions{BatchBytes: 16 << 10, StatementBytes: 4 << 10, StatementRows: 100})
	require.NoError(t, err)

	image := openSQLite(t, imagePath, false)
	assert.Equal(t, 7, report.Tables)
	assert.Equal(t, countRows(t, image, "parent")+countRows(t, image, "child")+countRows(t, image, "audit")+countRows(t, image, "keyed")+countRows(t, image, "gen")+countRows(t, image, "odd name"), report.Rows)
	assert.Greater(t, report.Batches, 10, "the copy is shipped as many bounded batches")

	// schema: the same objects with the same DDL, nothing of the old target left
	assert.Equal(t, schemaDump(t, image), schemaDump(t, target.db))
	for _, table := range []string{"parent", "child", "audit", "keyed", "gen", "empty_table", "odd name", "sqlite_sequence"} {
		assert.Equal(t, tableDump(t, image, table), tableDump(t, target.db, table), "table %s", table)
	}
	// generated columns are recomputed, not copied
	assert.Equal(t, queryStrings(t, image, `SELECT quote(x), quote(y), quote(z) FROM gen ORDER BY x`), queryStrings(t, target.db, `SELECT quote(x), quote(y), quote(z) FROM gen ORDER BY x`))
	// storage classes survive: text stays text, blobs stay blobs, and the
	// text that is not UTF-8 keeps its bytes
	assert.Equal(t, queryStrings(t, image, `SELECT typeof(name), typeof(ratio), typeof(payload), COUNT(*) FROM parent GROUP BY 1, 2, 3 ORDER BY 1, 2, 3`),
		queryStrings(t, target.db, `SELECT typeof(name), typeof(ratio), typeof(payload), COUNT(*) FROM parent GROUP BY 1, 2, 3 ORDER BY 1, 2, 3`))
	assert.Equal(t, []string{"text|FF00FE"}, queryStrings(t, target.db, `SELECT typeof(name), hex(name) FROM parent WHERE key = 100000`))

	// every batch is one transaction opened by the deferred-constraints pragma,
	// every statement is text protobuf can carry, and the budgets hold: no
	// row of the image is larger than a statement and no statement larger
	// than a batch, so nothing exceeds the configured sizes
	for i, batch := range target.batches {
		require.NotEmpty(t, batch)
		assert.Equal(t, deferForeignKeysSQL, batch[0].Sql, "batch %d", i)
		for _, stmt := range batch {
			assert.True(t, utf8.ValidString(stmt.Sql), "batch %d carries a statement that is not UTF-8", i)
			assert.LessOrEqual(t, len(stmt.Sql), 4<<10, "batch %d: a statement grew past the statement budget", i)
		}
		assert.LessOrEqual(t, batchBytes(batch), 16<<10+len(deferForeignKeysSQL), "batch %d grew past the batch budget", i)
	}
	assert.Equal(t, target.maxBatchBytes, report.MaxBatchBytes)
	// the trigger of the image did not fire during the copy: the audit rows
	// are the image's, not doubled
	assert.Equal(t, countRows(t, image, "audit"), countRows(t, target.db, "audit"))
	// the copy order followed the foreign keys, not the creation order
	assert.Less(t, firstInsertBatch(t, target.batches, "parent"), firstInsertBatch(t, target.batches, "child"))
}

func TestCopyDatabaseIsRepeatable(t *testing.T) {
	ctx := context.Background()
	imagePath := buildImage(t)
	target := &sqliteTarget{db: openSQLite(t, filepath.Join(t.TempDir(), "target.db"), true)}
	_, err := CopyDatabase(ctx, imagePath, target, CopyOptions{})
	require.NoError(t, err)
	// a retry after a completed (or half done) copy starts from the reset and
	// ends in the same state
	_, err = CopyDatabase(ctx, imagePath, target, CopyOptions{})
	require.NoError(t, err)
	image := openSQLite(t, imagePath, false)
	assert.Equal(t, schemaDump(t, image), schemaDump(t, target.db))
	assert.Equal(t, tableDump(t, image, "parent"), tableDump(t, target.db, "parent"))
	assert.Equal(t, tableDump(t, image, "sqlite_sequence"), tableDump(t, target.db, "sqlite_sequence"))
}

func TestCopyDatabaseReportsBatchFailures(t *testing.T) {
	imagePath := buildImage(t)
	target := &sqliteTarget{db: openSQLite(t, filepath.Join(t.TempDir(), "target.db"), false), failAt: 3, err: errors.New("partition is fenced for another owner")}
	_, err := CopyDatabase(context.Background(), imagePath, target, CopyOptions{BatchBytes: 16 << 10, StatementRows: 100})
	require.ErrorContains(t, err, "fenced for another owner")
	assert.Len(t, target.batches, 3, "the copy stops at the failed batch")
}

func TestCopyDatabaseStopsWhenTheContextIsCancelled(t *testing.T) {
	imagePath := buildImage(t)
	ctx, cancel := context.WithCancel(context.Background())
	target := &sqliteTarget{db: openSQLite(t, filepath.Join(t.TempDir(), "target.db"), false)}
	target.afterExecute = func(n int) {
		if n == 2 {
			cancel()
		}
	}
	_, err := CopyDatabase(ctx, imagePath, target, CopyOptions{BatchBytes: 16 << 10, StatementRows: 100})
	require.ErrorIs(t, err, context.Canceled)
	assert.LessOrEqual(t, len(target.batches), 3, "no further batch is shipped once the context is done")
}

func TestCopyDatabaseRejectsAnImageThatIsNotADatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "garbage.db")
	require.NoError(t, os.WriteFile(path, []byte("SQLite format 3\x00 but nothing else"), 0o600))
	target := &sqliteTarget{db: openSQLite(t, filepath.Join(t.TempDir(), "target.db"), false)}
	_, err := CopyDatabase(context.Background(), path, target, CopyOptions{})
	require.Error(t, err)
	assert.Empty(t, target.batches, "nothing is executed on the target for an unreadable image")
}

func TestOrderTablesFollowsForeignKeys(t *testing.T) {
	tables := []tableInfo{
		{name: "grandchild", parents: []string{"child"}},
		{name: "child", parents: []string{"parent", "external", "parent"}},
		{name: "parent"},
		{name: "loner"},
	}
	ordered, err := orderTables(tables)
	require.NoError(t, err)
	var names []string
	for _, tbl := range ordered {
		names = append(names, tbl.name)
	}
	assert.Equal(t, []string{"parent", "child", "grandchild", "loner"}, names)

	_, err = orderTables([]tableInfo{{name: "tree", parents: []string{"tree"}}})
	require.ErrorIs(t, err, ErrInvalidBundle, "a self reference cannot be ordered")
	assert.ErrorContains(t, err, `"tree"`)

	_, err = orderTables([]tableInfo{
		{name: "loner"},
		{name: "cycle_a", parents: []string{"cycle_b"}},
		{name: "cycle_b", parents: []string{"cycle_c"}},
		{name: "cycle_c", parents: []string{"cycle_a"}},
	})
	require.ErrorIs(t, err, ErrInvalidBundle, "a reference cycle cannot be ordered")
	assert.ErrorContains(t, err, "cycle_a, cycle_b, cycle_c")
}

func TestLiteralKeepsNonUTF8TextAsText(t *testing.T) {
	assert.Equal(t, "'plain'", literal("'plain'"))
	assert.Equal(t, "42", literal("42"))
	assert.Equal(t, "X'00ff'", literal("X'00ff'"))
	assert.Equal(t, "NULL", literal("NULL"))
	assert.Equal(t, "CAST(X'ff276127' AS TEXT)", literal("'\xff''a'''"))
}

func TestCopyDatabaseRefusesRowsOverTheBudget(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wide.db")
	db := openSQLite(t, path, false)
	_, err := db.Exec(`CREATE TABLE wide(id INTEGER PRIMARY KEY, body TEXT, blob BLOB)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO wide(body, blob) VALUES (?, ?), (?, ?)`, "small", []byte{1}, strings.Repeat("v", 3000), make([]byte, 5000))
	require.NoError(t, err)

	target := &sqliteTarget{db: openSQLite(t, filepath.Join(t.TempDir(), "target.db"), false)}
	seedStaleTarget(t, target.db)
	before := schemaDump(t, target.db)
	_, err = CopyDatabase(context.Background(), path, target, CopyOptions{MaxRowBytes: 8000})
	require.ErrorIs(t, err, zenerr.ErrResourceLimit)
	assert.ErrorContains(t, err, "table wide of the image holds a row of 8001 bytes")
	assert.Empty(t, target.batches, "an oversized image is refused before the target is touched")
	assert.Equal(t, before, schemaDump(t, target.db))

	report, err := CopyDatabase(context.Background(), path, target, CopyOptions{MaxRowBytes: 8001})
	require.NoError(t, err, "a row exactly at the budget is copied")
	assert.Equal(t, int64(2), report.Rows)
}

// TestCopyDatabaseBoundsStatementsAndBatches checks the size guarantees of
// CopyOptions with rows close to the statement budget and one row well past
// the batch budget: a statement never exceeds the budget unless a single row
// does, a batch never exceeds the budget unless a single statement does, and
// the oversized row travels alone.
func TestCopyDatabaseBoundsStatementsAndBatches(t *testing.T) {
	const (
		statementBudget = 4 << 10
		batchBudget     = 16 << 10
		largeRow        = 40 << 10
	)
	path := filepath.Join(t.TempDir(), "sized.db")
	db := openSQLite(t, path, false)
	_, err := db.Exec(`CREATE TABLE sized(id INTEGER PRIMARY KEY, body TEXT, blob BLOB)`)
	require.NoError(t, err)
	tx, err := db.Begin()
	require.NoError(t, err)
	for i := 0; i < 60; i++ {
		// ~3 KiB rendered: two of them do not fit into one statement
		_, err := tx.Exec(`INSERT INTO sized(body, blob) VALUES (?, ?)`, strings.Repeat("b", 2000), make([]byte, 500))
		require.NoError(t, err)
	}
	// a blob renders in hex at twice its size
	_, err = tx.Exec(`INSERT INTO sized(body, blob) VALUES (?, ?)`, "large", make([]byte, largeRow/2))
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	target := &sqliteTarget{db: openSQLite(t, filepath.Join(t.TempDir(), "target.db"), false)}
	report, err := CopyDatabase(context.Background(), path, target, CopyOptions{BatchBytes: batchBudget, StatementBytes: statementBudget, StatementRows: 100})
	require.NoError(t, err)
	assert.Equal(t, int64(61), report.Rows)

	var largeStatements int
	for i, batch := range target.batches {
		size := batchBudget + len(deferForeignKeysSQL)
		for _, stmt := range batch[1:] {
			if len(stmt.Sql) > statementBudget {
				largeStatements++
				assert.Contains(t, stmt.Sql, "'large'")
				assert.Len(t, batch, 2, "batch %d: the oversized row travels in a batch of its own", i)
				size = len(stmt.Sql) + len(deferForeignKeysSQL)
			}
		}
		assert.LessOrEqual(t, batchBytes(batch), size, "batch %d grew past its budget", i)
	}
	assert.Equal(t, 1, largeStatements, "only the row larger than the statement budget yields a larger statement")
	assert.Equal(t, target.maxBatchBytes, report.MaxBatchBytes)
	assert.Greater(t, report.MaxBatchBytes, largeRow)
	assert.Less(t, report.MaxBatchBytes, largeRow+1024)
	assert.Equal(t, tableDump(t, openSQLite(t, path, false), "sized"), tableDump(t, target.db, "sized"))
}

// TestCopyDatabaseCreatesUniqueIndexesBeforeTheRows covers a foreign key whose
// parent key is unique through a separately created index only: with foreign
// keys enforced, the child rows can only be inserted once that index exists.
func TestCopyDatabaseCreatesUniqueIndexesBeforeTheRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "indexed.db")
	db := openSQLite(t, path, false)
	for _, query := range []string{
		`CREATE TABLE account(id INTEGER PRIMARY KEY, code TEXT NOT NULL)`,
		`CREATE UNIQUE INDEX account_code ON account(code)`,
		`CREATE INDEX account_id_code ON account(id, code)`,
		`CREATE TABLE entry(id INTEGER PRIMARY KEY, account_code TEXT NOT NULL REFERENCES account(code))`,
		`INSERT INTO account(code) VALUES ('a'), ('b')`,
		`INSERT INTO entry(account_code) VALUES ('a'), ('b'), ('a')`,
	} {
		_, err := db.Exec(query)
		require.NoError(t, err, query)
	}

	target := &sqliteTarget{db: openSQLite(t, filepath.Join(t.TempDir(), "target.db"), true)}
	report, err := CopyDatabase(context.Background(), path, target, CopyOptions{BatchBytes: 1, StatementRows: 1})
	require.NoError(t, err)
	assert.Equal(t, int64(5), report.Rows)
	assert.Equal(t, schemaDump(t, db), schemaDump(t, target.db))
	assert.Equal(t, tableDump(t, db, "entry"), tableDump(t, target.db, "entry"))
	assert.Less(t, firstStatementBatch(t, target.batches, "CREATE UNIQUE INDEX account_code"), firstInsertBatch(t, target.batches, "entry"))
	assert.Greater(t, firstStatementBatch(t, target.batches, "CREATE INDEX account_id_code"), firstInsertBatch(t, target.batches, "entry"),
		"an index that no foreign key can rely on is created after the rows")
}

func TestCopyDatabaseRefusesReferenceCycles(t *testing.T) {
	for name, ddl := range map[string][]string{
		"self reference": {`CREATE TABLE tree(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES tree(id))`},
		"cycle": {
			`CREATE TABLE a(id INTEGER PRIMARY KEY, b_id INTEGER REFERENCES b(id))`,
			`CREATE TABLE b(id INTEGER PRIMARY KEY, a_id INTEGER REFERENCES a(id))`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "cyclic.db")
			db := openSQLite(t, path, false)
			for _, query := range ddl {
				_, err := db.Exec(query)
				require.NoError(t, err, query)
			}
			target := &sqliteTarget{db: openSQLite(t, filepath.Join(t.TempDir(), "target.db"), true)}
			seedStaleTarget(t, target.db)
			before := schemaDump(t, target.db)
			_, err := CopyDatabase(context.Background(), path, target, CopyOptions{})
			require.ErrorIs(t, err, ErrInvalidBundle)
			assert.Empty(t, target.batches, "an unsupported image is refused before the target is touched")
			assert.Equal(t, before, schemaDump(t, target.db))
		})
	}
}

// TestCopyDatabaseMemoryStaysBounded copies an image far larger than the
// batch size, holding one row far larger than the batch as well, and checks
// that the live heap never grows with the image and that no batch grows past
// what the largest row dictates: this is the regression guard for the restore
// that exhausted a node's memory. It measures the heap the copy retains
// between batches (after a GC), not the transient peak of the receive,
// protobuf or raft path; the bounded-batch guarantee is what keeps those
// proportional to one batch instead of the image.
func TestCopyDatabaseMemoryStaysBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a large image")
	}
	path := filepath.Join(t.TempDir(), "large.db")
	db, err := sql.Open("sqlite3", "file:"+path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`CREATE TABLE payload(key INTEGER PRIMARY KEY, body TEXT, blob BLOB)`)
	require.NoError(t, err)
	tx, err := db.Begin()
	require.NoError(t, err)
	body := strings.Repeat("<bpmn:task id=\"t\"/>", 200) // ~4 KiB of text
	blob := make([]byte, 1024)
	for i := 0; i < 40_000; i++ { // ~200 MiB of rows
		_, err := tx.Exec(`INSERT INTO payload(body, blob) VALUES (?, ?)`, body, blob)
		require.NoError(t, err)
	}
	// one row of 6 MiB, larger than the batch budget on its own, is copied
	// as a statement and a batch of its own
	const largeRow = 6 << 20
	_, err = tx.Exec(`INSERT INTO payload(body, blob) VALUES (?, ?)`, strings.Repeat("x", largeRow), nil)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	require.NoError(t, db.Close())

	// the live heap is sampled after every batch: garbage is collected first
	// so that only what the copy still holds is measured
	var peak uint64
	sample := func(int) {
		runtime.GC()
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		if ms.HeapAlloc > peak {
			peak = ms.HeapAlloc
		}
	}
	target := &sqliteTarget{db: openSQLite(t, filepath.Join(t.TempDir(), "target.db"), false), discard: true, afterExecute: sample}
	report, err := CopyDatabase(context.Background(), path, target, CopyOptions{})
	require.NoError(t, err)
	assert.Equal(t, int64(40_001), report.Rows)
	assert.Greater(t, report.Batches, 40)
	// a batch is at most 4 MiB of SQL unless a single row is larger: the
	// large row alone decides the largest batch
	assert.Greater(t, report.MaxBatchBytes, largeRow)
	assert.Less(t, report.MaxBatchBytes, largeRow+64<<10, "the largest batch holds the large row and little else")
	assert.Equal(t, report.MaxBatchBytes, target.maxBatchBytes)
	// the image is ~200 MiB, so a live heap that never exceeds 32 MiB shows
	// the copy holds nothing of the image between batches
	assert.Less(t, peak, uint64(32<<20), "live heap peaked at %d MiB while copying a %d-row image", peak>>20, report.Rows)
}

// sqliteTarget is a RestoreTarget over a plain SQLite file that executes
// every batch as one transaction, like the partition store does.
type sqliteTarget struct {
	db      *sql.DB
	batches [][]*rqcmd.Statement
	// discard drops executed batches instead of keeping them for inspection
	discard       bool
	executed      int
	maxBatchBytes int
	failAt        int
	err           error
	afterExecute  func(n int)
}

func (s *sqliteTarget) SchemaObjects(ctx context.Context) (tables, views []string, err error) {
	rows, err := s.db.QueryContext(ctx, `SELECT type, name FROM sqlite_master WHERE type IN ('table', 'view') AND (name NOT LIKE 'sqlite\_%' ESCAPE '\' OR name = 'sqlite_sequence') ORDER BY rowid`)
	if err != nil {
		return nil, nil, err
	}
	defer zenerr.CloseJoin(rows, &err, "schema rows")
	for rows.Next() {
		var typ, name string
		if err := rows.Scan(&typ, &name); err != nil {
			return nil, nil, err
		}
		if typ == "table" {
			tables = append(tables, name)
		} else {
			views = append(views, name)
		}
	}
	return tables, views, rows.Err()
}

func (s *sqliteTarget) Execute(ctx context.Context, statements []*rqcmd.Statement) error {
	s.executed++
	s.maxBatchBytes = max(s.maxBatchBytes, batchBytes(statements))
	if !s.discard {
		s.batches = append(s.batches, statements)
	}
	if s.failAt > 0 && s.executed == s.failAt {
		return s.err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	for _, stmt := range statements {
		if _, err := tx.ExecContext(ctx, stmt.Sql); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("%s: %w", truncateSQL(stmt.Sql), err)
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if s.afterExecute != nil {
		s.afterExecute(s.executed)
	}
	return nil
}

// batchBytes is the SQL size of a batch, the pragma opening it included.
func batchBytes(statements []*rqcmd.Statement) int {
	n := 0
	for _, stmt := range statements {
		n += len(stmt.Sql)
	}
	return n
}

func truncateSQL(sql string) string {
	if len(sql) > 120 {
		return sql[:120] + "..."
	}
	return sql
}

func openSQLite(t *testing.T, path string, foreignKeys bool) *sql.DB {
	t.Helper()
	dsn := "file:" + path
	if foreignKeys {
		dsn += "?_fk=1"
	}
	db, err := sql.Open("sqlite3", dsn)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// buildImage writes a database exercising what a copy has to get right: a
// child table created before its parent, AUTOINCREMENT with sqlite_sequence,
// a WITHOUT ROWID table, generated columns, an empty table, quoted names,
// indexes, a view with an INSTEAD OF trigger, a trigger that must not fire
// during the copy, and values of every storage class including text that is
// not UTF-8.
func buildImage(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "image.db")
	db, err := sql.Open("sqlite3", "file:"+path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	exec := func(query string, args ...any) {
		_, err := db.Exec(query, args...)
		require.NoError(t, err, query)
	}
	exec(`CREATE TABLE child(id INTEGER PRIMARY KEY AUTOINCREMENT, parent_key INTEGER NOT NULL REFERENCES parent(key), note TEXT)`)
	exec(`CREATE TABLE parent(key INTEGER PRIMARY KEY, name TEXT, ratio REAL, payload BLOB, flag INTEGER, "quoted ""col""" TEXT)`)
	exec(`CREATE TABLE audit(n INTEGER)`)
	exec(`CREATE TRIGGER parent_audit AFTER INSERT ON parent BEGIN INSERT INTO audit(n) VALUES (NEW.key); END`)
	exec(`CREATE TABLE keyed(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID`)
	exec(`CREATE TABLE gen(x INTEGER, y INTEGER GENERATED ALWAYS AS (x * 2) VIRTUAL, z INTEGER GENERATED ALWAYS AS (x + 1) STORED)`)
	exec(`CREATE TABLE empty_table(a, b)`)
	exec(`CREATE TABLE "odd name"("weird ""col""" TEXT, "select" INTEGER)`)
	exec(`CREATE INDEX parent_name ON parent(name)`)
	exec(`CREATE UNIQUE INDEX child_note ON child(note)`)
	exec(`CREATE VIEW parent_keys AS SELECT key FROM parent`)
	exec(`CREATE TRIGGER parent_keys_ins INSTEAD OF INSERT ON parent_keys BEGIN INSERT INTO parent(key) VALUES (NEW.key); END`)

	tx, err := db.Begin()
	require.NoError(t, err)
	for i := 0; i < 1200; i++ {
		var (
			name    any = fmt.Sprintf("name-%d 'quoted' \"dq\" ü漢字\n", i)
			ratio   any = float64(i) / 7
			payload any = []byte{byte(i), 0, 255, byte(i >> 8)}
			flag    any = i % 2
		)
		switch i % 10 {
		case 0:
			name, ratio, payload, flag = nil, nil, nil, nil
		case 1:
			name, payload = "", []byte{}
		case 2:
			ratio = 1e300
		case 3:
			ratio = -0.1
		case 4:
			ratio = math.MaxFloat64
		case 5:
			ratio = math.SmallestNonzeroFloat64
		case 6:
			flag = math.MaxInt64
		case 7:
			flag = math.MinInt64
		}
		_, err := tx.Exec(`INSERT INTO parent(key, name, ratio, payload, flag, "quoted ""col""") VALUES (?, ?, ?, ?, ?, ?)`, i*3-1000, name, ratio, payload, flag, fmt.Sprintf("q%d", i))
		require.NoError(t, err)
	}
	_, err = tx.Exec(`INSERT INTO parent(key, name) VALUES (100000, CAST(X'ff00fe' AS TEXT))`)
	require.NoError(t, err)
	for i := 0; i < 2500; i++ {
		_, err := tx.Exec(`INSERT INTO child(parent_key, note) VALUES (?, ?)`, (i%1200)*3-1000, fmt.Sprintf("note-%d", i))
		require.NoError(t, err)
	}
	for i := 0; i < 5; i++ {
		_, err := tx.Exec(`INSERT INTO keyed(k, v) VALUES (?, ?)`, fmt.Sprintf("k%d", i), i)
		require.NoError(t, err)
	}
	for i := 1; i <= 3; i++ {
		_, err := tx.Exec(`INSERT INTO gen(x) VALUES (?)`, i)
		require.NoError(t, err)
	}
	_, err = tx.Exec(`INSERT INTO "odd name" VALUES ('o', 1), (NULL, 2)`)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	require.NoError(t, db.Close())
	return path
}

// seedStaleTarget gives the target objects a restore must get rid of: a
// table and a view the image does not have, an AUTOINCREMENT counter, and
// tables of the image with a different shape and content. The child table
// references the parent table, so with foreign keys enforced the reset has
// to drop them in the right order.
func seedStaleTarget(t *testing.T, db *sql.DB) {
	t.Helper()
	for _, query := range []string{
		`CREATE TABLE stale(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)`,
		`INSERT INTO stale(v) VALUES ('x'), ('y')`,
		`CREATE VIEW stale_view AS SELECT * FROM stale`,
		`CREATE TABLE parent(key INTEGER PRIMARY KEY, other TEXT)`,
		`INSERT INTO parent VALUES (1, 'old')`,
		`CREATE TABLE child(id INTEGER PRIMARY KEY, parent_key INTEGER REFERENCES parent(key))`,
		`INSERT INTO child VALUES (1, 1)`,
		`CREATE TABLE migration(name TEXT PRIMARY KEY, ran_at INTEGER)`,
	} {
		_, err := db.Exec(query)
		require.NoError(t, err, query)
	}
}

func schemaDump(t *testing.T, db *sql.DB) []string {
	t.Helper()
	return queryStrings(t, db, `SELECT type, name, tbl_name, quote(sql) FROM sqlite_master WHERE name NOT LIKE 'sqlite\_%' ESCAPE '\' ORDER BY type, name`)
}

func tableDump(t *testing.T, db *sql.DB, table string) []string {
	t.Helper()
	columns, err := tableColumns(context.Background(), db, table)
	require.NoError(t, err)
	selects := make([]string, len(columns))
	order := make([]string, len(columns))
	for i, c := range columns {
		selects[i] = "quote(" + quoteIdent(c) + ")"
		order[i] = fmt.Sprint(i + 1)
	}
	return queryStrings(t, db, "SELECT "+strings.Join(selects, ", ")+" FROM "+quoteIdent(table)+" ORDER BY "+strings.Join(order, ", "))
}

// queryStrings returns every row of query as its columns joined by '|'.
func queryStrings(t *testing.T, db *sql.DB, query string) []string {
	t.Helper()
	rows, err := db.Query(query)
	require.NoError(t, err, query)
	defer func() { require.NoError(t, rows.Close()) }()
	columns, err := rows.Columns()
	require.NoError(t, err)
	out := []string{}
	for rows.Next() {
		values := make([]sql.NullString, len(columns))
		scan := make([]any, len(columns))
		for i := range values {
			scan[i] = &values[i]
		}
		require.NoError(t, rows.Scan(scan...))
		parts := make([]string, len(values))
		for i, v := range values {
			parts[i] = v.String
		}
		out = append(out, strings.Join(parts, "|"))
	}
	require.NoError(t, rows.Err())
	return out
}

func countRows(t *testing.T, db *sql.DB, table string) int64 {
	t.Helper()
	var n int64
	require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM "+quoteIdent(table)).Scan(&n))
	return n
}

// firstInsertBatch returns the index of the first batch inserting into table.
func firstInsertBatch(t *testing.T, batches [][]*rqcmd.Statement, table string) int {
	t.Helper()
	return firstStatementBatch(t, batches, "INSERT INTO "+quoteIdent(table)+" ")
}

// firstStatementBatch returns the index of the first batch holding a
// statement that starts with prefix.
func firstStatementBatch(t *testing.T, batches [][]*rqcmd.Statement, prefix string) int {
	t.Helper()
	for i, batch := range batches {
		for _, stmt := range batch {
			if strings.HasPrefix(stmt.Sql, prefix) {
				return i
			}
		}
	}
	t.Fatalf("no batch holds a statement starting with %q", prefix)
	return -1
}
