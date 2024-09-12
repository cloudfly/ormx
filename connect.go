package ormx

import (
	"context"
	"fmt"
	"time"

	"github.com/cloudfly/flagx"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog"
)

var (
	databaseDsn     = flagx.NewString("database.dsn", "", "the dsn address of the master database which ormx connect to write")
	databaseDsnRead = flagx.NewString("database.dsn.read", "", "the dsn address of the slave database which ormx connect to read")
	dbDriver        = flagx.NewString("database.driver", "mysql", "the sql driver for executing query")
	lifetime        = flagx.NewDuration("database.conn.lifetime", "10m", "the maximum amount of seconds a connection may be reused")
	idletime        = flagx.NewDuration("database.conn.idletime", "1m", "the maximum amount of seconds a connection may be idle")
	maxOpen         = flagx.NewInt("database.conn.maxopen", 100, "the maximum number of connections to the database server")
	maxIdle         = flagx.NewInt("database.conn.maxidle", 32, "the maximum number of connections in idle connection poll")
)

var (
	db  *sqlx.DB
	rdb *sqlx.DB
)

// Connect to the database server by using the addr and password specified in flags
func Connect(ctx context.Context) error {
	if *databaseDsn == "" {
		return nil
	}

	var (
		err error
	)

	zerolog.Ctx(ctx).Info().Str("dsn", *databaseDsn).Msg("Connecting to master database server")
	db, err = sqlx.ConnectContext(ctx, *dbDriver, *databaseDsn)
	if err != nil {
		return fmt.Errorf("connect error: %w", err)
	}
	db.SetConnMaxLifetime(time.Duration(lifetime.Msecs) * time.Millisecond)
	db.SetConnMaxIdleTime(time.Duration(idletime.Msecs) * time.Millisecond)
	db.SetMaxIdleConns(*maxIdle)
	db.SetMaxOpenConns(*maxOpen)

	if *databaseDsnRead == "" {
		zerolog.Ctx(ctx).Info().Str("dsn", *databaseDsnRead).Msg("Connecting to slave database server")
		rdb, err = sqlx.ConnectContext(ctx, *dbDriver, *databaseDsnRead)
		if err != nil {
			return fmt.Errorf("connect error: %w", err)
		}
		rdb.SetConnMaxLifetime(time.Duration(lifetime.Msecs) * time.Millisecond)
		rdb.SetConnMaxIdleTime(time.Duration(idletime.Msecs) * time.Millisecond)
		rdb.SetMaxIdleConns(*maxIdle)
		rdb.SetMaxOpenConns(*maxOpen)
	}

	return nil
}

// DefaultProvider return the sqlx.DB created by Connect()
func DefaultProvider(isMaster bool) *sqlx.DB {
	if !isMaster && rdb != nil {
		return rdb
	}
	return db
}

// Close the connections to the database server in driver
func Close() error {
	if db != nil {
		return db.Close()
	}
	return nil
}
