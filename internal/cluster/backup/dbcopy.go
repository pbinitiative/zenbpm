package backup

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"

	_ "github.com/mattn/go-sqlite3" // registers the sqlite3 driver that reads the spooled image

	"github.com/pbinitiative/zenbpm/internal/cluster/zenerr"
	"github.com/pbinitiative/zenbpm/internal/log"
	rqcmd "github.com/rqlite/rqlite/v10/command/proto"
)

// RestoreTarget is the partition database a restore image is copied into.
// Execute applies one batch of statements as a single transaction (one entry
// of the partition's raft log); SchemaObjects lists the tables and views the
// target currently holds, sqlite_sequence included when it exists.
type RestoreTarget interface {
	SchemaObjects(ctx context.Context) (tables, views []string, err error)
	Execute(ctx context.Context, statements []*rqcmd.Statement) error
}

// CopyOptions bounds the statement batches CopyDatabase ships to the target.
// Zero values take the defaults.
//
// The bounds hold together: a statement never exceeds StatementBytes unless a
// single row renders larger, a batch never exceeds BatchBytes unless a single
// statement is larger, and a row renders as at most about twice its bytes
// (blobs and text that is not UTF-8 are spelled in hex) plus a few bytes per
// column. With MaxRowBytes checked on the image before the target is touched,
// one batch, and so one raft entry and the memory the copy holds at any time,
// is bounded by max(BatchBytes, 2*MaxRowBytes) plus that small overhead.
type CopyOptions struct {
	// BatchBytes is the accumulated SQL size of one batch. A statement that
	// would push a batch past it starts the next batch.
	BatchBytes int
	// StatementBytes caps the SQL size of one multi-row INSERT. A row that
	// would push a statement past it starts the next statement.
	StatementBytes int
	// StatementRows caps the rows of one multi-row INSERT.
	StatementRows int
	// MaxRowBytes caps the bytes of one row of the image, the sum of the byte
	// lengths of its values. Every table is checked before the target is
	// reset; an image holding a larger row is refused with
	// zenerr.ErrResourceLimit.
	MaxRowBytes int64
}

const (
	defaultCopyBatchBytes     = 4 << 20
	defaultCopyStatementBytes = 1 << 20
	defaultCopyStatementRows  = 500
	defaultCopyMaxRowBytes    = 32 << 20

	// deferForeignKeysSQL opens every batch: constraints are checked when the
	// batch commits, so the order of rows inside a batch does not matter when
	// the store enforces foreign keys. It is a no-op when it does not.
	deferForeignKeysSQL = "PRAGMA defer_foreign_keys = ON"
)

func (o CopyOptions) withDefaults() CopyOptions {
	pick := func(v, d int) int {
		if v <= 0 {
			return d
		}
		return v
	}
	maxRow := o.MaxRowBytes
	if maxRow <= 0 {
		maxRow = defaultCopyMaxRowBytes
	}
	return CopyOptions{
		BatchBytes:     pick(o.BatchBytes, defaultCopyBatchBytes),
		StatementBytes: pick(o.StatementBytes, defaultCopyStatementBytes),
		StatementRows:  pick(o.StatementRows, defaultCopyStatementRows),
		MaxRowBytes:    maxRow,
	}
}

// CopyReport summarises a finished copy.
type CopyReport struct {
	Tables  int
	Rows    int64
	Batches int
	// MaxBatchBytes is the SQL size of the largest batch executed.
	MaxBatchBytes int
}

// CopyDatabase replaces the content of target with the SQLite database at
// path. The image is read row by row and shipped as bounded statement
// batches (see CopyOptions), so the memory needed does not grow with the size
// of the database: a whole image never has to fit into memory or into one
// raft entry, and the largest row of the image, not its size, decides the
// largest batch.
//
// The image is checked before the target is touched: its schema has to be
// supported and no row may exceed the row budget. The target is then reset
// (views, then tables that are not part of the image, then the image's
// tables children first), the image's tables are created from their original
// DDL together with the unique indexes a foreign key may rely on, their rows
// are copied parents first, and the remaining indexes, views and triggers are
// created last so that triggers never fire during the copy. Values keep
// their storage class: every value is spelled as a SQL literal by SQLite
// itself. sqlite_sequence is copied when a table of the image uses
// AUTOINCREMENT.
//
// Every batch is one transaction with deferred foreign key checks, so a row
// may only reference rows of an earlier batch or of its own. Tables are
// ordered parents first to guarantee that, which is impossible for a table
// that references itself or takes part in a reference cycle: such an image
// is refused with ErrInvalidBundle before the target is reset.
//
// A failure leaves the target partially written; the caller records the
// restore as having modified data and a retry starts over from the reset.
func CopyDatabase(ctx context.Context, path string, target RestoreTarget, opts CopyOptions) (report CopyReport, err error) {
	opts = opts.withDefaults()
	src, err := sql.Open("sqlite3", "file:"+path+"?mode=ro&immutable=1")
	if err != nil {
		return report, fmt.Errorf("failed to open restore image: %w", err)
	}
	defer zenerr.CloseJoin(src, &err, "restore image")
	// the image is read by one connection; a second one would only open
	// the file again
	src.SetMaxOpenConns(1)

	schema, err := readImageSchema(ctx, src)
	if err != nil {
		return report, err
	}
	if err := checkImageRows(ctx, src, schema, opts.MaxRowBytes); err != nil {
		return report, err
	}
	batch := &statementBatch{ctx: ctx, target: target, opts: opts}

	if err := resetTarget(ctx, target, schema, batch); err != nil {
		return report, fmt.Errorf("failed to reset partition database: %w", err)
	}
	for _, t := range schema.tables {
		if err := batch.add(t.sql); err != nil {
			return report, fmt.Errorf("failed to create table %s: %w", t.name, err)
		}
	}
	for _, o := range schema.preData {
		if err := batch.add(o.sql); err != nil {
			return report, fmt.Errorf("failed to create %s %s: %w", o.typ, o.name, err)
		}
	}
	if err := batch.flush(); err != nil {
		return report, fmt.Errorf("failed to create tables: %w", err)
	}

	for _, t := range schema.tables {
		rows, err := copyTableRows(ctx, src, t, batch)
		if err != nil {
			return report, fmt.Errorf("failed to copy table %s: %w", t.name, err)
		}
		report.Tables++
		report.Rows += rows
		log.Debug("restore copied table %s: %d rows", t.name, rows)
	}
	if schema.copySequence {
		if err := batch.add("DELETE FROM sqlite_sequence"); err != nil {
			return report, fmt.Errorf("failed to reset sqlite_sequence: %w", err)
		}
		if _, err := copyTableRows(ctx, src, tableInfo{name: "sqlite_sequence", columns: []string{"name", "seq"}}, batch); err != nil {
			return report, fmt.Errorf("failed to copy sqlite_sequence: %w", err)
		}
	}
	for _, o := range schema.postData {
		if err := batch.add(o.sql); err != nil {
			return report, fmt.Errorf("failed to create %s %s: %w", o.typ, o.name, err)
		}
	}
	if err := batch.flush(); err != nil {
		return report, fmt.Errorf("failed to create indexes, views and triggers: %w", err)
	}
	report.Batches = batch.executed
	report.MaxBatchBytes = batch.maxBytes
	return report, nil
}

// schemaObject is one row of the image's sqlite_master.
type schemaObject struct {
	typ  string
	name string
	sql  string
}

// tableInfo is a table of the image with its visible columns and the tables
// its foreign keys reference.
type tableInfo struct {
	name    string
	sql     string
	columns []string
	parents []string
}

type imageSchema struct {
	// tables in copy order: parents before their children, creation order
	// otherwise
	tables []tableInfo
	// preData holds the unique indexes, created before the rows: a foreign
	// key may rely on one for the uniqueness of its parent key, and a
	// foreign-key-enforcing target refuses child rows while it is missing
	preData []schemaObject
	// postData holds the other indexes, the views and the triggers in
	// creation order, views before triggers so that INSTEAD OF triggers find
	// their view
	postData []schemaObject
	// copySequence is set when a table uses AUTOINCREMENT, in which case the
	// image's sqlite_sequence is copied as well
	copySequence bool
}

var (
	autoincrementRe = regexp.MustCompile(`(?i)\bautoincrement\b`)
	uniqueIndexRe   = regexp.MustCompile(`(?is)^\s*create\s+unique\s+index\b`)
)

// readImageSchema reads the user objects of the image in creation order.
func readImageSchema(ctx context.Context, src *sql.DB) (*imageSchema, error) {
	rows, err := src.QueryContext(ctx, `SELECT type, name, sql FROM sqlite_master WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite\_%' ESCAPE '\' ORDER BY rowid`)
	if err != nil {
		return nil, fmt.Errorf("failed to read image schema: %w", err)
	}
	var objects []schemaObject
	for rows.Next() {
		var o schemaObject
		if err := rows.Scan(&o.typ, &o.name, &o.sql); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("failed to read image schema: %w", err)
		}
		objects = append(objects, o)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, fmt.Errorf("failed to read image schema: %w", err)
	}

	schema := &imageSchema{}
	var tables []tableInfo
	var uniqueIndexes, indexes, views, triggers []schemaObject
	hasSequence := false
	for _, o := range objects {
		switch o.typ {
		case "table":
			t := tableInfo{name: o.name, sql: o.sql}
			if t.columns, err = tableColumns(ctx, src, o.name); err != nil {
				return nil, err
			}
			if t.parents, err = tableParents(ctx, src, o.name); err != nil {
				return nil, err
			}
			tables = append(tables, t)
			if autoincrementRe.MatchString(o.sql) {
				hasSequence = true
			}
		case "index":
			if uniqueIndexRe.MatchString(o.sql) {
				uniqueIndexes = append(uniqueIndexes, o)
			} else {
				indexes = append(indexes, o)
			}
		case "view":
			views = append(views, o)
		case "trigger":
			triggers = append(triggers, o)
		default:
			return nil, fmt.Errorf("%w: image contains unsupported schema object %q of type %q", ErrInvalidBundle, o.name, o.typ)
		}
	}
	if hasSequence {
		var n int
		if err := src.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'sqlite_sequence'`).Scan(&n); err != nil {
			return nil, fmt.Errorf("failed to read image schema: %w", err)
		}
		schema.copySequence = n > 0
	}
	if schema.tables, err = orderTables(tables); err != nil {
		return nil, err
	}
	schema.preData = uniqueIndexes
	schema.postData = append(append(indexes, views...), triggers...)
	return schema, nil
}

// checkImageRows refuses an image holding a row of more than maxRowBytes,
// measured as the sum of the byte lengths of the row's values. The check
// reads lengths only, never a rendered value, and runs before the target is
// touched so that an oversized image fails without modifying anything.
func checkImageRows(ctx context.Context, src *sql.DB, schema *imageSchema, maxRowBytes int64) error {
	for _, t := range schema.tables {
		if len(t.columns) == 0 {
			continue
		}
		lengths := make([]string, len(t.columns))
		for i, c := range t.columns {
			lengths[i] = "COALESCE(length(CAST(" + quoteIdent(c) + " AS BLOB)), 0)"
		}
		var largest sql.NullInt64
		if err := src.QueryRowContext(ctx, "SELECT MAX("+strings.Join(lengths, " + ")+") FROM "+quoteIdent(t.name)).Scan(&largest); err != nil {
			return fmt.Errorf("failed to measure rows of %s: %w", t.name, err)
		}
		if largest.Valid && largest.Int64 > maxRowBytes {
			return fmt.Errorf("%w: table %s of the image holds a row of %d bytes, over the limit of %d bytes per row", zenerr.ErrResourceLimit, t.name, largest.Int64, maxRowBytes)
		}
	}
	return nil
}

// tableColumns returns the columns of table that an INSERT may set: hidden
// and generated columns are left out.
func tableColumns(ctx context.Context, src *sql.DB, table string) ([]string, error) {
	rows, err := src.QueryContext(ctx, "PRAGMA table_xinfo("+quoteIdent(table)+")")
	if err != nil {
		return nil, fmt.Errorf("failed to read columns of %s: %w", table, err)
	}
	var columns []string
	for rows.Next() {
		var (
			cid, notNull, pk, hidden int
			name, typ                string
			dflt                     any
		)
		if err := rows.Scan(&cid, &name, &typ, &notNull, &dflt, &pk, &hidden); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("failed to read columns of %s: %w", table, err)
		}
		if hidden == 0 {
			columns = append(columns, name)
		}
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, fmt.Errorf("failed to read columns of %s: %w", table, err)
	}
	return columns, nil
}

// tableParents returns the tables referenced by the foreign keys of table.
func tableParents(ctx context.Context, src *sql.DB, table string) ([]string, error) {
	rows, err := src.QueryContext(ctx, "PRAGMA foreign_key_list("+quoteIdent(table)+")")
	if err != nil {
		return nil, fmt.Errorf("failed to read foreign keys of %s: %w", table, err)
	}
	var parents []string
	for rows.Next() {
		var (
			id, seq                          int
			parent, from, onUpdate, onDelete string
			to, match                        any
		)
		if err := rows.Scan(&id, &seq, &parent, &from, &to, &onUpdate, &onDelete, &match); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("failed to read foreign keys of %s: %w", table, err)
		}
		parents = append(parents, parent)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, fmt.Errorf("failed to read foreign keys of %s: %w", table, err)
	}
	return parents, nil
}

// orderTables sorts tables so that every table follows the tables its foreign
// keys reference, keeping creation order otherwise. References to tables
// outside the image impose no order. A table referencing itself or a set of
// tables referencing each other in a cycle cannot be ordered: their rows
// would have to be copied within one transaction, which the bounded batches
// do not guarantee, so such an image is refused as unsupported.
func orderTables(tables []tableInfo) ([]tableInfo, error) {
	index := make(map[string]int, len(tables))
	for i, t := range tables {
		index[t.name] = i
	}
	pending := make([]int, len(tables))
	children := make([][]int, len(tables))
	for i, t := range tables {
		seen := map[int]bool{}
		for _, parent := range t.parents {
			p, ok := index[parent]
			if !ok || seen[p] {
				continue
			}
			if p == i {
				return nil, fmt.Errorf("%w: table %q of the image references itself, which a batched copy cannot restore", ErrInvalidBundle, t.name)
			}
			seen[p] = true
			pending[i]++
			children[p] = append(children[p], i)
		}
	}
	ordered := make([]tableInfo, 0, len(tables))
	done := make([]bool, len(tables))
	for len(ordered) < len(tables) {
		next := -1
		for i := range tables {
			if !done[i] && pending[i] == 0 {
				next = i
				break
			}
		}
		if next == -1 {
			var cycle []string
			for i := range tables {
				if !done[i] {
					cycle = append(cycle, tables[i].name)
				}
			}
			return nil, fmt.Errorf("%w: tables %s of the image reference each other in a cycle, which a batched copy cannot restore", ErrInvalidBundle, strings.Join(cycle, ", "))
		}
		done[next] = true
		ordered = append(ordered, tables[next])
		for _, c := range children[next] {
			pending[c]--
		}
	}
	return ordered, nil
}

// resetTarget drops what the target holds: views first, then the tables the
// image does not know (they can only reference tables of the image, never
// the other way round), then the image's tables with children before their
// parents. sqlite_sequence cannot be dropped and is emptied instead.
func resetTarget(ctx context.Context, target RestoreTarget, schema *imageSchema, batch *statementBatch) error {
	tables, views, err := target.SchemaObjects(ctx)
	if err != nil {
		return err
	}
	for _, v := range views {
		if err := batch.add("DROP VIEW IF EXISTS " + quoteIdent(v)); err != nil {
			return err
		}
	}
	inImage := make(map[string]bool, len(schema.tables))
	for _, t := range schema.tables {
		inImage[t.name] = true
	}
	existing := make(map[string]bool, len(tables))
	hasSequence := false
	for _, t := range tables {
		existing[t] = true
		if t == "sqlite_sequence" {
			hasSequence = true
			continue
		}
		if inImage[t] {
			continue
		}
		if err := batch.add("DROP TABLE IF EXISTS " + quoteIdent(t)); err != nil {
			return err
		}
	}
	for i := len(schema.tables) - 1; i >= 0; i-- {
		if !existing[schema.tables[i].name] {
			continue
		}
		if err := batch.add("DROP TABLE IF EXISTS " + quoteIdent(schema.tables[i].name)); err != nil {
			return err
		}
	}
	if hasSequence {
		if err := batch.add("DELETE FROM sqlite_sequence"); err != nil {
			return err
		}
	}
	return batch.flush()
}

// copyTableRows streams the rows of t into multi-row INSERT statements. Every
// value is rendered by SQLite's quote(), which spells integers, reals, text,
// blobs and NULL as literals that reproduce the value with its storage class.
// A row is rendered on its own first, so that a statement is closed before a
// row that would push it past the statement budget: only a single row larger
// than the budget yields a larger statement.
func copyTableRows(ctx context.Context, src *sql.DB, t tableInfo, batch *statementBatch) (total int64, err error) {
	if len(t.columns) == 0 {
		return 0, nil
	}
	selects := make([]string, len(t.columns))
	quoted := make([]string, len(t.columns))
	for i, c := range t.columns {
		quoted[i] = quoteIdent(c)
		// quote() renders text up to its first NUL byte; text holding one is
		// spelled as a hex blob cast back to TEXT, which keeps every byte
		selects[i] = "CASE WHEN typeof(" + quoted[i] + ") = 'text' AND instr(CAST(" + quoted[i] + " AS BLOB), X'00') > 0" +
			" THEN 'CAST(X''' || hex(" + quoted[i] + ") || ''' AS TEXT)' ELSE quote(" + quoted[i] + ") END"
	}
	rows, err := src.QueryContext(ctx, "SELECT "+strings.Join(selects, ", ")+" FROM "+quoteIdent(t.name)) // #nosec G202 -- identifiers come from the image's own schema and are quoted by quoteIdent
	if err != nil {
		return 0, err
	}
	defer zenerr.CloseJoin(rows, &err, "rows of "+t.name)

	prefix := "INSERT INTO " + quoteIdent(t.name) + " (" + strings.Join(quoted, ", ") + ") VALUES "
	values := make([]string, len(t.columns))
	scan := make([]any, len(t.columns))
	for i := range values {
		scan[i] = &values[i]
	}
	var (
		stmt, row strings.Builder
		n         int
	)
	flushStatement := func() error {
		if n == 0 {
			return nil
		}
		err := batch.add(stmt.String())
		stmt.Reset()
		n = 0
		return err
	}
	for rows.Next() {
		if err := rows.Scan(scan...); err != nil {
			return total, err
		}
		row.Reset()
		row.WriteByte('(')
		for i, v := range values {
			if i > 0 {
				row.WriteByte(',')
			}
			row.WriteString(literal(v))
		}
		row.WriteByte(')')
		if n > 0 && (n >= batch.opts.StatementRows || stmt.Len()+1+row.Len() > batch.opts.StatementBytes) {
			if err := flushStatement(); err != nil {
				return total, err
			}
		}
		if n == 0 {
			stmt.WriteString(prefix)
		} else {
			stmt.WriteByte(',')
		}
		stmt.WriteString(row.String())
		n++
		total++
	}
	if err := rows.Err(); err != nil {
		return total, err
	}
	if err := flushStatement(); err != nil {
		return total, err
	}
	return total, batch.flush()
}

// literal returns the SQL literal for a value rendered by quote(). Statement
// text travels as a protobuf string and therefore has to be UTF-8; text
// holding other bytes is spelled as a hex blob cast back to TEXT, which stores
// the same bytes with the same storage class.
func literal(quoted string) string {
	if utf8.ValidString(quoted) {
		return quoted
	}
	if len(quoted) < 2 || quoted[0] != '\'' || quoted[len(quoted)-1] != '\'' {
		// only text can carry arbitrary bytes; anything else is a bug in quote()
		return quoted
	}
	raw := strings.ReplaceAll(quoted[1:len(quoted)-1], "''", "'")
	return "CAST(X'" + hex.EncodeToString([]byte(raw)) + "' AS TEXT)"
}

// quoteIdent quotes a schema identifier.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// statementBatch accumulates statements and executes them on the target as
// one transaction. A statement that would push the batch past its budget is
// executed with the next batch: only a single statement larger than the
// budget yields a larger batch.
type statementBatch struct {
	ctx      context.Context
	target   RestoreTarget
	opts     CopyOptions
	stmts    []*rqcmd.Statement
	bytes    int
	executed int
	maxBytes int
}

func (b *statementBatch) add(sql string) error {
	if len(b.stmts) > 0 && b.bytes+len(sql) > b.opts.BatchBytes {
		if err := b.flush(); err != nil {
			return err
		}
	}
	b.stmts = append(b.stmts, &rqcmd.Statement{Sql: sql})
	b.bytes += len(sql)
	return nil
}

func (b *statementBatch) flush() error {
	if len(b.stmts) == 0 {
		return nil
	}
	stmts := append([]*rqcmd.Statement{{Sql: deferForeignKeysSQL}}, b.stmts...)
	size := b.bytes + len(deferForeignKeysSQL)
	b.stmts, b.bytes = nil, 0
	if err := b.ctx.Err(); err != nil {
		return err
	}
	if err := b.target.Execute(b.ctx, stmts); err != nil {
		return err
	}
	b.executed++
	b.maxBytes = max(b.maxBytes, size)
	return nil
}
