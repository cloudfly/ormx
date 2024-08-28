package ormx

import (
	"context"
	"database/sql/driver"

	"github.com/jmoiron/sqlx"
)

var (
	p DBProvider
)

// DBProvider
type DBProvider func(isMaster bool) *sqlx.DB

// RunTxContext execute a transiction
func RunTxContext(ctx context.Context, f func(ctx context.Context, tx *sqlx.Tx) error) error {
	db := Master()
	tx, err := db.BeginTxx(ctx, nil)
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

// Exec execute a sql on master DB
func Exec(ctx context.Context, sql string, args ...interface{}) (driver.Result, error) {
	log.Printf(ctx, InfoLevel, "Executing sql: %s, args: %v", sql, args)
	emitMetric(ctx, sql)
	return Master().ExecContext(ctx, sql, args...)
}

// Exec execute a sql in transaction
func ExecTx(ctx context.Context, tx *sqlx.Tx, sql string, args ...interface{}) (driver.Result, error) {
	log.Printf(ctx, InfoLevel, "Executing sql: %s, args: %v", sql, args)
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
		log.Printf(ctx, InfoLevel, "Selecting on master: %s, args: %v", sql, args)
	} else {
		db = Slave()
		log.Printf(ctx, DebugLevel, "Selecting on slave: %s, args: %v", sql, args)
	}
	emitMetric(ctx, sql)
	return db.SelectContext(ctx, dest, sql, args...)
}

// Select will query data into dest with raw sql and args.
//
// it will auto query from master if the context having FromMaster
func SelectTx(ctx context.Context, tx *sqlx.Tx, dest interface{}, sql string, args ...interface{}) error {
	log.Printf(ctx, InfoLevel, "Selecting sql: %s, args: %v", sql, args)
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
		log.Printf(ctx, InfoLevel, "Getting by sql: %s, args: %v", sql, args)
	} else {
		db = Slave()
		log.Printf(ctx, DebugLevel, "Getting by sql: %s, args: %v", sql, args)
	}
	emitMetric(ctx, sql)
	return db.GetContext(ctx, dest, sql, args...)
}

// Get will get one data from tx by using raw sql and args.
func GetTx(ctx context.Context, tx *sqlx.Tx, dest interface{}, sql string, args ...interface{}) error {
	log.Printf(ctx, InfoLevel, "Getting by sql: %s, args: %v", sql, args)
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
