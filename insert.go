package ormx

import (
	"context"
	"database/sql/driver"
	"fmt"
	"reflect"
	"slices"

	sb "github.com/huandu/go-sqlbuilder"
	"github.com/jmoiron/sqlx"
)

// InsertIgnore insert new data into database and ingore the rows on duplicate keys
func InsertIgnore(ctx context.Context, table string, data ...any) error {
	return InsertIgnoreTx(ctx, nil, table, data...)
}

// InsertIgnoreTx insert new data into database and ingore the rows on duplicate keys using transaction
func InsertIgnoreTx(ctx context.Context, tx *sqlx.Tx, table string, data ...any) error {
	if len(data) == 0 {
		return nil
	}
	var (
		err error
	)
	if table == "" {
		table = TableName(data)
	}
	ib, err := NewInsertBuilderFromStruct(ctx, table, data[0])
	if err != nil {
		return fmt.Errorf("create insert builder from structure error: %w", err)
	}
	ib = ib.InsertIgnoreInto(table)
	sql, args := Build(ctx, ib)

	if tx == nil {
		_, err = Exec(ctx, sql, args...)
	} else {
		_, err = ExecTx(ctx, tx, sql, args...)
	}
	if err != nil {
		return fmt.Errorf("exec error: %w", err)
	}
	return nil
}

// InsertManyTx insert rows in transaction, the all data type should be same structure.
func InsertMany(ctx context.Context, table string, data ...any) error {
	return InsertManyTx(ctx, nil, table, data...)
}

// InsertManyTx insert rows in transaction, the all data type should be same structure.
func InsertManyTx(ctx context.Context, tx *sqlx.Tx, table string, data ...any) error {
	if len(data) == 0 {
		return nil
	}
	var (
		err error
	)
	if table == "" {
		table = TableName(data)
	}
	ib, err := NewInsertBuilderFromStruct(ctx, table, data[0])
	if err != nil {
		return fmt.Errorf("create insert builder from structure error: %w", err)
	}
	sql, args := Build(ctx, ib)

	if tx == nil {
		_, err = Exec(ctx, sql, args...)
	} else {
		_, err = ExecTx(ctx, tx, sql, args...)
	}
	if err != nil {
		return fmt.Errorf("exec error: %w", err)
	}
	return nil
}

// InsertOneTx insert rows into table, the data type should be structure.
func InsertOne(ctx context.Context, table string, data any) (int64, error) {
	return InsertOneTx(ctx, nil, table, data)
}

// InsertOneTx insert rows in transaction, the data type should be structure.
func InsertOneTx(ctx context.Context, tx *sqlx.Tx, table string, data any) (int64, error) {
	if data == nil {
		return 0, nil
	}
	var (
		err error
		id  int64
		r   driver.Result
	)
	if table == "" {
		table = TableName(data)
	}
	ib, err := NewInsertBuilderFromStruct(ctx, table, data)
	if err != nil {
		return 0, fmt.Errorf("create insert builder from structure error: %w", err)
	}
	sql, args := Build(ctx, ib)

	if tx == nil {
		r, err = Exec(ctx, sql, args...)
	} else {
		r, err = ExecTx(ctx, tx, sql, args...)
	}
	if err != nil {
		return 0, fmt.Errorf("exec error: %w", err)
	}
	id, err = r.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("get last insert id: %w", err)
	}
	return id, nil
}

// NewInsertBuilderFromStruct create a new insert builder from data, the struct field with 'insert' option in field tag will be inserted
//
// such as: db:"columnName,insert" or db:",insert"
//
// the struct field with no insert tag option, will be ignored
func NewInsertBuilderFromStruct(ctx context.Context, table string, data ...any) (*sb.InsertBuilder, error) {
	if len(data) <= 0 {
		return nil, fmt.Errorf("no data to insert")
	}
	if table == "" {
		table = TableName(data[0])
	}

	// 使用第一个数据的类型，获取列名信息。
	var (
		ib        = sb.NewInsertBuilder().InsertInto(table)
		t         = dereferencedType(reflect.TypeOf(data[0]))
		cols      []string
		fieldTags = make([]string, t.NumField())
	)
	for i := 0; i < t.NumField(); i++ {
		fieldType := t.Field(i)
		name, after := colNameFromTag(fieldType)
		if name == "" {
			continue
		}
		opts := ParseOptionStr(after)
		if _, ok := opts["insert"]; !ok {
			continue
		}
		cols = append(cols, name)

		if t := opts["type"]; t != "" {
			fieldTags[i] = t
		} else {
			fieldTags[i] = "-"
		}
	}

	if len(cols) == 0 {
		return nil, fmt.Errorf(`no insert field defined in '%s' type, defined db:",insert" for insert field`, t.Name())
	}

	injectNamespace := namespaceValueForInject(ctx)
	shouldInject := injectNamespace != "" && !slices.Contains(cols, namespaceColumnName)
	if shouldInject {
		cols = append(cols, namespaceColumnName)
	}

	ib.Cols(cols...)

	for _, item := range data {
		var (
			v    = dereferencedValue(reflect.ValueOf(item))
			vals []any
		)
		if !v.IsValid() || v.IsZero() {
			continue
		}
		for i := 0; i < v.NumField(); i++ {
			field := v.Field(i)
			if fieldTags[i] != "" {
				vals = append(vals, convertValueByDBType(dereferencedValue(field).Interface(), fieldTags[i]))
			}
		}
		if shouldInject {
			vals = append(vals, injectNamespace)
		}
		ib.Values(vals...)
	}

	return ib, nil
}
