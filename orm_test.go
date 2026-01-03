package ormx

import (
	"context"
	"time"
)

func init() {
	if err := Init(context.TODO(), nil); err != nil {
		panic(err)
	}
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
