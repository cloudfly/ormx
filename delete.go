package ormx

import (
	"context"

	sb "github.com/huandu/go-sqlbuilder"
	"github.com/jmoiron/sqlx"
)

// DeleteWhere delete rows that match the filter from the given table
func DeleteWhere(ctx context.Context, table string, filter KVs) error {
	return DeleteWhereTx(ctx, nil, table, filter)
}

// DeleteWhereTx delete rows that match the filter in transaction from the given table
func DeleteWhereTx(ctx context.Context, tx *sqlx.Tx, table string, filter KVs) error {
	builder := sb.NewDeleteBuilder().DeleteFrom(table)
	builder = builder.Where(WhereFromKVs(&builder.Cond, filter, nil)...)
	var (
		sql, args = Build(ctx, builder)
		err       error
	)
	if tx == nil {
		_, err = Exec(ctx, sql, args...)
	} else {
		_, err = ExecTx(ctx, tx, sql, args...)
	}
	return err
}

// DeleteWhere delete rows by id from the given table
func DeleteByID(ctx context.Context, table string, id ...any) error {
	return DeleteByIDTx(ctx, nil, table, id...)
}

// DeleteWhere delete rows by id in transaction from the table
func DeleteByIDTx(ctx context.Context, tx *sqlx.Tx, table string, id ...any) error {
	builder := sb.NewDeleteBuilder().DeleteFrom(table)
	builder = builder.Where(WhereFrom(&builder.Cond, id, nil)...)
	var (
		err  error
		sql  string
		args []any
	)
	sql, args = Build(ctx, builder)
	if tx == nil {
		_, err = Exec(ctx, sql, args...)
	} else {
		_, err = ExecTx(ctx, tx, sql, args...)
	}
	return err
}
