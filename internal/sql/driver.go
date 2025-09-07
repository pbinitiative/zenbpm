// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/pbinitiative/zenbpm/internal/log"
	"github.com/rqlite/rqlite/v8/command/proto"
)

type Rows struct {
	columns   []string
	types     []string
	values    []*proto.Values
	rowNumber int64 // -1 is the default, indicating Next() has not been called
	err       error
	ctx       context.Context
}

type Row struct {
	columns []string
	types   []string
	values  *proto.Values
	err     error
	ctx     context.Context
}

const (
	ErrNoRows = "No result row"
)

func ConstructRows(ctx context.Context, columns []string, types []string, values []*proto.Values) *Rows {
	return &Rows{
		columns:   columns,
		types:     types,
		values:    values,
		rowNumber: -1,
		ctx:       ctx,
	}
}

func ConstructRow(ctx context.Context, columns []string, types []string, values *proto.Values, err error) *Row {
	return &Row{
		columns: columns,
		types:   types,
		values:  values,
		ctx:     ctx,
		err:     err,
	}
}

func ConstructRowFromRows(ctx context.Context, rows *Rows, err error) *Row {
	return &Row{
		columns: rows.columns,
		types:   rows.types,
		values:  rows.values[0],
		ctx:     ctx,
		err:     err,
	}
}

// Next positions the QueryResult result pointer so that Scan() or Map() is ready.
//
// You should call Next() first, but gorqlite will fix it if you call Map() or Scan() before
// the initial Next().
//
// A common idiom:
//
//	rows := conn.Write(something)
//	for rows.Next() {
//	    // your Scan/Map and processing here.
//	}
func (qr *Rows) Next() bool {
	if qr.rowNumber >= int64(len(qr.values)-1) {
		return false
	}

	qr.rowNumber += 1
	return true
}

func (rs *Rows) Close() error {
	return nil
}

func (qr *Rows) Scan(dest ...any) error {

	if qr.rowNumber == -1 {
		return errors.New("you need to Next() before you Scan(), sorry, it's complicated")
	}

	if qr.rowNumber >= int64(len(qr.values)) {
		return errors.New("no more rows")
	}

	thisRowValues := qr.values[qr.rowNumber]
	err := Scan(qr.ctx, qr.columns, thisRowValues, dest...)
	if err != nil {
		return err
	}
	return nil

}

func Scan(ctx context.Context, columns []string, values *proto.Values, dest ...any) error {
	if len(dest) != len(columns) {
		return fmt.Errorf("expected %d columns but got %d vars", len(columns), len(dest))
	}
	for n, d := range dest {
		src := values.Parameters[n]
		switch d := d.(type) {
		case *time.Time:
			if src == nil {
				continue
			}
			t, err := toTime(src)
			if err != nil {
				return fmt.Errorf("%v: bad time col:(%d/%s) val:%v", err, n, columns[n], src)
			}
			*d = t
		case *int:
			switch x := src.GetValue().(type) {
			case *proto.Parameter_I:
				*d = int(x.I)
			case *proto.Parameter_D:
				*d = int(x.D)
			case *proto.Parameter_S:
				i, err := strconv.Atoi(x.S)
				if err != nil {
					return err
				}
				*d = i
			case nil:
				log.Debugf(ctx, "skipping nil scan data for variable #%d (%s)", n, columns[n])
			default:
				return fmt.Errorf("invalid int col:%d type:%T val:%v", n, src, src)
			}
		case *int32:
			switch x := src.GetValue().(type) {
			case *proto.Parameter_I:
				*d = int32(x.I)
			case *proto.Parameter_D:
				*d = int32(x.D)
			case *proto.Parameter_S:
				i, err := strconv.ParseInt(x.S, 10, 32)
				if err != nil {
					return err
				}
				*d = int32(i)
			case nil:
				log.Debugf(ctx, "skipping nil scan data for variable #%d (%s)", n, columns[n])
			default:
				return fmt.Errorf("invalid int col:%d type:%T val:%v", n, src, src)
			}
		case *int64:
			switch x := src.GetValue().(type) {
			case *proto.Parameter_I:
				*d = x.I
			case *proto.Parameter_D:
				*d = int64(x.D)
			case *proto.Parameter_S:
				i, err := strconv.Atoi(x.S)
				if err != nil {
					return err
				}
				*d = int64(i)
			case nil:
				log.Debugf(ctx, "skipping nil scan data for variable #%d (%s)", n, columns[n])
			default:
				return fmt.Errorf("invalid int col:%d type:%T val:%v", n, src, src)
			}
		case *float64:
			switch x := src.GetValue().(type) {
			case *proto.Parameter_D:
				*d = x.D
			case *proto.Parameter_I:
				*d = float64(x.I)
			case *proto.Parameter_S:
				f, err := strconv.ParseFloat(x.S, 64)
				if err != nil {
					return err
				}
				*d = f
			case nil:
				log.Debugf(ctx, "skipping nil scan data for variable #%d (%s)", n, columns[n])
			default:
				return fmt.Errorf("invalid float64 col:%d type:%T val:%v", n, src, src)
			}
		case *string:
			switch x := src.GetValue().(type) {
			case *proto.Parameter_S:
				*d = x.S
			case nil:
				log.Debugf(ctx, "skipping nil scan data for variable #%d (%s)", n, columns[n])
			default:
				return fmt.Errorf("invalid string col:%d type:%T val:%v", n, src, src)
			}
		case *bool:
			// Note: Rqlite does not support bool, but this is a loop from dest
			// meaning, the user might be targeting to a bool-type variable.
			// Per Go convention, and per strconv.ParseBool documentation, bool might be
			// coming from value of "1", "t", "T", "TRUE", "true", "True", for `true` and
			// "0", "f", "F", "FALSE", "false", "False" for `false`
			switch x := src.GetValue().(type) {
			case *proto.Parameter_I:
				b, err := strconv.ParseBool(strconv.FormatInt(x.I, 10))
				if err != nil {
					return err
				}
				*d = b
			case *proto.Parameter_D:
				b, err := strconv.ParseBool(strconv.FormatFloat(x.D, 'g', -1, 64))
				if err != nil {
					return err
				}
				*d = b
			case *proto.Parameter_S:
				b, err := strconv.ParseBool(x.S)
				if err != nil {
					return err
				}
				*d = b
			case nil:
				log.Debugf(ctx, "skipping nil scan data for variable #%d (%s)", n, columns[n])
			default:
				return fmt.Errorf("invalid bool col:%d type:%T val:%v", n, src, src)
			}
		case *[]byte:
			switch x := src.GetValue().(type) {
			case *proto.Parameter_Y:
				*d = x.Y
			case *proto.Parameter_S:
				*d = []byte(x.S)
			case nil:
				log.Debugf(ctx, "skipping nil scan data for variable #%d (%s)", n, columns[n])
			default:
				return fmt.Errorf("invalid []byte col:%d type:%T val:%v", n, src, src)
			}

		// case *NullString:
		// 	switch src := src.(type) {
		// 	case string:
		// 		*d = gorqlite.NullString{Valid: true, String: src}
		// 	case nil:
		// 		*d = NullString{Valid: false}
		// 	default:
		// 		return fmt.Errorf("invalid string col:%d type:%T val:%v", n, src, src)
		// 	}
		case *sql.NullInt64:
			switch x := src.GetValue().(type) {
			case *proto.Parameter_D:
				*d = sql.NullInt64{Valid: true, Int64: int64(x.D)}
			case *proto.Parameter_I:
				*d = sql.NullInt64{Valid: true, Int64: x.I}
			case *proto.Parameter_S:
				i, err := strconv.Atoi(x.S)
				if err != nil {
					return err
				}
				*d = sql.NullInt64{Valid: true, Int64: int64(i)}
			case nil:
				*d = sql.NullInt64{Valid: false}
			default:
				return fmt.Errorf("invalid int64 col:%d type:%T val:%v", n, src, src)
			}
		// case *NullInt32:
		// 	switch src := src.(type) {
		// 	case float64:
		// 		*d = NullInt32{Valid: true, Int32: int32(src)}
		// 	case int64:
		// 		*d = NullInt32{Valid: true, Int32: int32(src)}
		// 	case string:
		// 		i, err := strconv.ParseInt(src, 10, 32)
		// 		if err != nil {
		// 			return err
		// 		}
		// 		*d = NullInt32{Valid: true, Int32: int32(i)}
		// 	case nil:
		// 		*d = NullInt32{Valid: false}
		// 	default:
		// 		return fmt.Errorf("invalid int32 col:%d type:%T val:%v", n, src, src)
		// 	}
		// case *NullInt16:
		// 	switch src := src.(type) {
		// 	case float64:
		// 		*d = NullInt16{Valid: true, Int16: int16(src)}
		// 	case int64:
		// 		*d = NullInt16{Valid: true, Int16: int16(src)}
		// 	case string:
		// 		i, err := strconv.ParseInt(src, 10, 16)
		// 		if err != nil {
		// 			return err
		// 		}
		// 		*d = NullInt16{Valid: true, Int16: int16(i)}
		// 	case nil:
		// 		*d = NullInt16{Valid: false}
		// 	default:
		// 		return fmt.Errorf("invalid int16 col:%d type:%T val:%v", n, src, src)
		// 	}
		// case *NullFloat64:
		// 	switch src := src.(type) {
		// 	case float64:
		// 		*d = NullFloat64{Valid: true, Float64: src}
		// 	case int64:
		// 		*d = NullFloat64{Valid: true, Float64: float64(src)}
		// 	case string:
		// 		f, err := strconv.ParseFloat(src, 64)
		// 		if err != nil {
		// 			return err
		// 		}
		// 		*d = NullFloat64{Valid: true, Float64: f}
		// 	case nil:
		// 		*d = NullFloat64{Valid: false}
		// 	default:
		// 		return fmt.Errorf("invalid float64 col:%d type:%T val:%v", n, src, src)
		// 	}
		// case *NullBool:
		// 	switch src := src.(type) {
		// 	case float64:
		// 		b, err := strconv.ParseBool(strconv.FormatFloat(src, 'g', -1, 64))
		// 		if err != nil {
		// 			return err
		// 		}
		// 		*d = NullBool{Valid: true, Bool: b}
		// 	case int64:
		// 		b, err := strconv.ParseBool(strconv.FormatInt(src, 10))
		// 		if err != nil {
		// 			return err
		// 		}
		// 		*d = NullBool{Valid: true, Bool: b}
		// 	case string:
		// 		b, err := strconv.ParseBool(src)
		// 		if err != nil {
		// 			return err
		// 		}
		// 		*d = NullBool{Valid: true, Bool: b}
		// 	case nil:
		// 		*d = NullBool{Valid: false}
		// 	default:
		// 		return fmt.Errorf("invalid bool col:%d type:%T val:%v", n, src, src)
		// 	}
		// case *NullTime:
		// 	if src == nil {
		// 		*d = NullTime{Valid: false}
		// 	} else {
		// 		t, err := toTime(src)
		// 		if err != nil {
		// 			return fmt.Errorf("%v: bad time col:(%d/%s) val:%v", err, n, qr.columns[n], src)
		// 		}
		// 		*d = NullTime{Valid: true, Time: t}
		// 	}
		default:
			return fmt.Errorf("unknown destination type (%T) to scan into in variable #%d", d, n)
		}
	}
	return nil
}

func toTime(src interface{}) (time.Time, error) {
	switch src := src.(type) {
	case string:
		const layout = "2006-01-02 15:04:05"
		if t, err := time.Parse(layout, src); err == nil {
			return t, nil
		}
		return time.Parse(time.RFC3339, src)
	case float64:
		return time.Unix(int64(src), 0), nil
	case int64:
		return time.Unix(src, 0), nil
	}
	return time.Time{}, fmt.Errorf("invalid time type:%T val:%v", src, src)
}

func (rs *Rows) Err() error {
	// As we know potential error right at the beging ater the QueryContext is called we dont need this
	return nil
}

func (r *Row) Scan(dest ...interface{}) error {
	if r.values == nil {
		return errors.New(ErrNoRows)
	}
	return Scan(r.ctx, r.columns, r.values, dest...)
}
