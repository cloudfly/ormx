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
	database     = flagx.NewString("database.name", "", "the database name ormx connect to")
	host         = flagx.NewString("database.host", "127.0.0.1", "the database server's host ormx connect to")
	port         = flagx.NewInt("database.port", 3306, "the database server's port ormx connect to")
	username     = flagx.NewString("database.username", "root", "the username used to connect to database server")
	password     = flagx.NewString("database.password", "123456", "the password used to connect to database server")
	dbDriver     = flagx.NewString("database.driver", "mysql", "the sql driver for executing query")
	timeout      = flagx.NewInt("database.timeout", 30, "the timeout for data from database server")
	readTimeout  = flagx.NewInt("database.read.timeout", 30, "the timeout for maximum seconds the driver will wait for a query finished")
	writeTimeout = flagx.NewInt("database.write.timeout", 30, "the timeout for maximum seconds the driver will wait for a execution finished")
	lifetime     = flagx.NewDuration("database.conn.lifetime", "10m", "the maximum amount of seconds a connection may be reused")
	idletime     = flagx.NewDuration("database.conn.idletime", "1m", "the maximum amount of seconds a connection may be idle")
	maxOpen      = flagx.NewInt("database.conn.maxopen", 100, "the maximum number of connections to the database server")
	maxIdle      = flagx.NewInt("database.conn.maxidle", 32, "the maximum number of connections in idle connection poll")
)

var (
	db *sqlx.DB
)

// Connect to the database server by using the addr and password specified in flags
func Connect(ctx context.Context) error {
	if *database == "" {
		return nil
	}

	var (
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local&timeout=%ds&readTimeout=%ds&writeTimeout=%ds", *username, *password, *host, *port, *database, *timeout, *readTimeout, *writeTimeout)
		err error
	)

	zerolog.Ctx(ctx).Info().Str("dsn", dsn).Msg("Connecting to database server")
	db, err = sqlx.ConnectContext(ctx, *dbDriver, dsn)
	if err != nil {
		return fmt.Errorf("connect error: %w", err)
	}

	db.SetConnMaxLifetime(time.Duration(lifetime.Msecs) * time.Millisecond)
	db.SetConnMaxIdleTime(time.Duration(idletime.Msecs) * time.Millisecond)
	db.SetMaxIdleConns(*maxIdle)
	db.SetMaxOpenConns(*maxOpen)

	return nil
}

// DefaultProvider return the sqlx.DB created by Connect()
func DefaultProvider(isMaster bool) *sqlx.DB {
	return db
}

// Close the connections to the database server in driver
func Close() error {
	if db != nil {
		return db.Close()
	}
	return nil
}
