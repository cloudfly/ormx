package ormx

import (
	"context"
	"fmt"
	"time"

	"github.com/cloudfly/ormx/test"
)

func init() {
	if err := Init(test.Provider, ""); err != nil {
		panic(err)
	}
	SetLogger(testLogger{})
}

type testLogger struct{}

func (l testLogger) Printf(ctx context.Context, level Level, format string, args ...any) {
	fmt.Printf(format+"\n", args...)
}

type TestRow struct {
	ID          int64     `db:"id"`
	Producer    string    `db:"producer,insert"`
	Resource    string    `db:"resource,insert"`
	Action      string    `db:"action,insert"`
	Message     string    `db:"message,insert"`
	CreatedTime time.Time `db:"created_time"`
	UpdatedTime time.Time `db:"updated_time"`
}

func (tr TestRow) Table() string {
	return "test"
}

type TestRowPatch struct {
	Action  *string `db:"action,insert"`
	Message *string `db:"message,insert"`
}
