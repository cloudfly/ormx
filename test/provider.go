package test

import (
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

var (
	db *sqlx.DB
)

func init() {
	var (
		user     = "root"
		password = "123456"
		addr     = "localhost:3306"
		database = "test"
	)
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=True&loc=Local&timeout=30s&readTimeout=30s&writeTimeout=30s", user, password, addr, database)
	var err error
	db, err = sqlx.Connect("mysql", dsn)
	if err != nil {
		panic(err)
	}
}

func Provider(master bool) *sqlx.DB {
	return db
}
