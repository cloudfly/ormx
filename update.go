package ormx

import (
	"context"
	"database/sql/driver"
	"reflect"

	sb "github.com/huandu/go-sqlbuilder"
	"github.com/jmoiron/sqlx"
)

// PatchByID updates the data by id in the table.
func PatchByID(ctx context.Context, table string, id int64, data any) error {
	return PatchByIDTx(ctx, nil, table, id, data)
}

// PatchByIDTx updates the data by id in the table using a transaction.
func PatchByIDTx(ctx context.Context, tx *sqlx.Tx, table string, id int64, data any) error {
	ub, ok := NewUpdateBuilderFromStruct(data, table)
	if !ok {
		return nil
	}
	ub = ub.Where(WhereFrom(&ub.Cond, id, nil)...)
	var (
		sql  string
		args []any
		err  error
	)
	sql, args = Build(ctx, ub)

	if tx == nil {
		_, err = Exec(ctx, sql, args...)
	} else {
		_, err = ExecTx(ctx, tx, sql, args...)
	}
	return err
}

// PatchWhere updates the data that match the filter in the table.
// The filter is used as the condition and can be of type KVs, or struct.
func PatchWhere(ctx context.Context, table string, data any, filter any) (int64, error) {
	return PatchWhereTx(ctx, nil, table, data, filter)
}

// PatchWhereTx updates the data that matchthe filter in the table using a transaction.
// The filter is used as the condition and can be of type KVs, struct, []int64, int64.
func PatchWhereTx(ctx context.Context, tx *sqlx.Tx, table string, data any, filter any) (int64, error) {
	ub, ok := NewUpdateBuilderFromStruct(data, table)
	if !ok {
		return 0, nil
	}
	ub = ub.Where(WhereFrom(&ub.Cond, filter, nil)...)
	var (
		err  error
		sql  string
		args []any
		r    driver.Result
	)
	sql, args = Build(ctx, ub)
	if tx == nil {
		r, err = Exec(ctx, sql, args...)
	} else {
		r, err = ExecTx(ctx, tx, sql, args...)
	}
	if err != nil {
		return 0, err
	}
	return r.RowsAffected()
}

// NewUpdateBuilderFromStruct 使用 data 数据定义 update builder
func NewUpdateBuilderFromStruct(data any, table string) (*sb.UpdateBuilder, bool) {
	if table == "" {
		table = TableName(data)
	}
	ub := sb.NewUpdateBuilder().Update(table)
	v := dereferencedValue(reflect.ValueOf(data))
	t := dereferencedType(reflect.TypeOf(data))
	assigned := false
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		fieldType := t.Field(i)
		if field.IsNil() {
			continue
		}

		name, after := colNameFromTag(fieldType)
		if name == "" {
			continue
		}
		opts := ParseOptionStr(after)
		fieldValue := convertValueByDBType(dereferencedValue(field).Interface(), opts["type"])
		ub = ub.SetMore(ub.Assign(name, fieldValue))
		assigned = true
	}
	return ub, assigned
}
