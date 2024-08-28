package ormx

import (
	"context"
	"fmt"
	"testing"
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

func TestSimple(t *testing.T) {

	var (
		ctx = context.Background()
		err error
		row = TestRow{
			Producer: "unittest",
			Resource: "insert",
			Action:   "test",
			Message:  "test message",
		}
		patch = TestRowPatch{}
	)

	t.Run("insert", func(t *testing.T) {
		row.ID, err = InsertOne(ctx, "", row)
		test.NoError(t, err)
		t.Log("the new row id is", row.ID)
	})

	t.Run("select", func(t *testing.T) {
		var row2 TestRow
		err = GetByID(ctx, &row2, "", row.ID)
		test.NoError(t, err)
		test.Equal(t, row.ID, row2.ID)
		test.Equal(t, row.Producer, row2.Producer)
		test.Equal(t, row.Resource, row2.Resource)
		test.Equal(t, row.Action, row2.Action)
		test.Equal(t, row.Message, row2.Message)
	})

	t.Run("update", func(t *testing.T) {
		newAction := "patch"
		patch.Action = &newAction
		err = PatchByID(ctx, row.Table(), row.ID, patch)
		test.NoError(t, err)

		var row2 TestRow
		// FromMaster, do not use cache
		err = GetByID(FromMaster(ctx), &row2, "", row.ID)
		test.NoError(t, err)
		t.Log(row2)
		test.Equal(t, row.ID, row2.ID)
		test.Equal(t, row.Producer, row2.Producer)
		test.Equal(t, row.Resource, row2.Resource)
		test.Equal(t, newAction, row2.Action)
		test.Equal(t, row.Message, row2.Message)
	})

	t.Run("delete", func(t *testing.T) {
		newAction := "patch"
		patch.Action = &newAction
		err = DeleteByID(ctx, row.Table(), row.ID)
		test.NoError(t, err)

		exist, err := Exist(ctx, row.Table(), row.ID)
		test.NoError(t, err)
		test.Equal(t, false, exist)
	})
}
