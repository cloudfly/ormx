package ormx

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog"
)

var (
	p DBProvider = DefaultProvider
)

// DBProvider
type DBProvider func(isMaster bool) *sqlx.DB

// RunTxContext execute a transiction
func RunTxContext(ctx context.Context, f func(ctx context.Context, tx *sqlx.Tx) error) error {
	return RunTxWithOptionContext(ctx, nil, f)
}

// RunTxContext execute a transiction with custom options
func RunTxWithOptionContext(ctx context.Context, opts *sql.TxOptions, f func(ctx context.Context, tx *sqlx.Tx) error) error {
	db := Master()
	tx, err := db.BeginTxx(ctx, opts)
	if err != nil {
		return err
	}

	if err := f(ctx, tx); err != nil {
		if err := tx.Rollback(); err != nil {
			return err
		}
		return err
	}

	return tx.Commit()
}

// RunInLock execute a sql in lock
func RunInLock(ctx context.Context, table string, f func(ctx context.Context) error) (err error) {
	db := Master()
	_, err = db.ExecContext(ctx, fmt.Sprintf("LOCK TABLES %s WRITE", table))
	if err != nil {
		return
	}
	// 确保锁最终释放（即使后续操作出错）
	defer func() {
		if _, e := db.ExecContext(ctx, "UNLOCK TABLES"); e != nil {
			err = e
		}
	}()
	err = f(ctx)
	return
}

func RunInRLock(ctx context.Context, table string, f func(ctx context.Context) error) (err error) {
	db := Master()
	_, err = db.ExecContext(ctx, "LOCK TABLES ? READ", table)
	if err != nil {
		return
	}
	// 确保锁最终释放（即使后续操作出错）
	defer func() {
		_, err = db.ExecContext(ctx, "UNLOCK TABLES")
	}()
	err = f(ctx)
	return
}

// Exec execute a sql on master DB
func Exec(ctx context.Context, sql string, args ...interface{}) (driver.Result, error) {
	zerolog.Ctx(ctx).Info().Str("query", sql).Any("args", args).Msg("Executing sql query")
	emitMetric(ctx, sql)
	return Master().ExecContext(ctx, sql, args...)
}

// Exec execute a sql in transaction
func ExecTx(ctx context.Context, tx *sqlx.Tx, sql string, args ...interface{}) (driver.Result, error) {
	zerolog.Ctx(ctx).Info().Str("query", sql).Any("args", args).Msg("Executing sql query")
	emitMetric(ctx, sql)
	return tx.ExecContext(ctx, sql, args...)
}

// Select will query data into dest with raw sql and args.
//
// it will auto query from master if the context having FromMaster
func Select(ctx context.Context, dest interface{}, sql string, args ...interface{}) error {
	var (
		db *sqlx.DB
	)
	if isFromMaster(ctx) {
		db = Master()
		zerolog.Ctx(ctx).Info().Str("query", sql).Any("args", args).Msg("Selecting on master")
	} else {
		db = Slave()
		zerolog.Ctx(ctx).Debug().Str("query", sql).Any("args", args).Msg("Selecting on slave")
	}
	emitMetric(ctx, sql)
	return db.SelectContext(ctx, dest, sql, args...)
}

// Select will query data into dest with raw sql and args.
//
// it will auto query from master if the context having FromMaster
func SelectTx(ctx context.Context, tx *sqlx.Tx, dest interface{}, sql string, args ...interface{}) error {
	zerolog.Ctx(ctx).Info().Str("query", sql).Any("args", args).Msg("Selecting in transaction")
	emitMetric(ctx, sql)
	return tx.SelectContext(ctx, dest, sql, args...)
}

// Get will get one data into dest with raw sql and args.
//
// it will auto query from master if the context having FromMaster
func Get(ctx context.Context, dest interface{}, sql string, args ...interface{}) error {
	var (
		db *sqlx.DB
	)
	if isFromMaster(ctx) {
		db = Master()
		zerolog.Ctx(ctx).Info().Str("query", sql).Any("args", args).Msg("Getting on master")
	} else {
		db = Slave()
		zerolog.Ctx(ctx).Debug().Str("query", sql).Any("args", args).Msg("Getting on slave")
	}
	emitMetric(ctx, sql)
	return db.GetContext(ctx, dest, sql, args...)
}

// Get will get one data from tx by using raw sql and args.
func GetTx(ctx context.Context, tx *sqlx.Tx, dest interface{}, sql string, args ...interface{}) error {
	zerolog.Ctx(ctx).Info().Str("query", sql).Any("args", args).Msg("Getting in transaction")
	emitMetric(ctx, sql)
	return tx.GetContext(ctx, dest, sql, args...)
}

// Master return master *sqlx.DB which returned by DBProvider, panic if DBProvider is not Initilized
func Master() *sqlx.DB {
	if p == nil {
		panic("db getter is nil, call ormx.Init to initilaze the DBGetter")
	}
	return p(true)
}

// Master return slave *sqlx.DB which returned by DBProvider, panic if DBProvider is not Initilized
func Slave() *sqlx.DB {
	if p == nil {
		panic("db getter is nil, call ormx.Init to initilaze the DBGetter")
	}
	return p(false)
}
