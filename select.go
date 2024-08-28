package ormx

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/cloudfly/ormx/cache"
	sb "github.com/huandu/go-sqlbuilder"
)

func GetByID(ctx context.Context, dst interface{}, table string, id int64) error {
	if table == "" {
		table = TableName(dst)
	}

	if !isFromMaster(ctx) {
		// Not reading data from the primary database indicates that some delay is tolerable.
		// Attempt to read from the local cache.
		if v, ok := cache.Get(table, id); ok {
			if content, ok := v.([]byte); ok {
				if err := json.Unmarshal(content, dst); err == nil {
					return nil
				} else {
					// Deserialization error indicates that the data is unusable. Delete it directly.
					cache.Remove(table, id)
				}
			}
		}
	}

	b, err := NewSelectBuilderFromStruct(table, dst)
	if err != nil {
		return fmt.Errorf("create select builder error:%w", err)
	}
	b = b.Where(WhereFrom(id, nil)...)

	var (
		statement string
		args      []any
	)
	statement, args = Build(ctx, b)

	if err := Get(ctx, dst, statement, args...); err != nil {
		return err
	}
	content, err := json.Marshal(dst)
	if err != nil {
		log.Printf(ctx, WarnLevel, "Failed to marshal data for caching: %w, data: %+v", err.Error(), dst)
		// 忽略序列化错误，顶多就是无法cache，无关紧要
		return nil
	}
	cache.Set(time.Second*10, table, id, content)
	return nil
}

// GetWhere 使用自定义条件跟新数据
func GetWhere(ctx context.Context, dst interface{}, table string, filter KVs) error {
	if table == "" {
		table = TableName(dst)
	}
	builder := sb.NewStruct(dst).SelectFrom(table).Where(WhereFrom(filter, nil)...)
	sql, args := Build(ctx, builder)
	return Get(ctx, dst, sql, args...)
}

// GetWhere 使用自定义条件跟新数据
func SelectWhere(ctx context.Context, dst interface{}, table string, filter KVs) error {
	if table == "" {
		table = TableName(dst)
	}
	builder, err := NewSelectBuilderFromStruct(table, dst)
	if err != nil {
		return fmt.Errorf("new select builder error: %w", err)
	}
	builder = builder.Where(WhereFrom(filter, nil)...)
	sql, args := Build(ctx, builder)

	return Select(ctx, dst, sql, args...)
}

// Count select the count of rows in table which match the filter condition
func Count(ctx context.Context, table string, filter KVs) (int64, error) {
	total := sql.NullInt64{}
	b := sb.NewSelectBuilder().Select("COUNT(1) as total").From(table)
	b = b.Where(WhereFromKVs(filter, nil)...)
	sql, args := Build(ctx, b)
	err := Get(ctx, &total, sql, args...)
	if IsNotFound(err) {
		err = nil
	}
	return total.Int64, err
}

// Exist return true if the at least one row found in table by using where condition
func Exist(ctx context.Context, table string, filter KVs) (bool, error) {
	n := sql.NullInt64{}
	b := sb.NewSelectBuilder().Select("1").From(table).Limit(1)
	b = b.Where(WhereFrom(filter, nil)...)
	statement, args := Build(ctx, b)
	err := Get(ctx, &n, statement, args...)
	if err != nil {
		if IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// NewSelectBuilderFromStruct create select sql builder by data
func NewSelectBuilderFromStruct(table string, data any) (*sb.SelectBuilder, error) {
	if table == "" {
		table = TableName(data)
	}
	b := sb.NewSelectBuilder().From(table)
	if data == nil {
		b = b.Select("*")
		return b, nil
	}
	t := dereferencedType(reflect.TypeOf(data))

	if t.Kind() == reflect.Slice {
		t = t.Elem()
	}

	cols := make([]string, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		fieldType := t.Field(i)
		name, after := colNameFromTag(fieldType)
		if name == "" {
			continue
		}
		opts := ParseOptionStr(after)
		if optv, ok := opts["select"]; ok && (optv == "-" || optv == "false") {
			continue
		}
		cols = append(cols, name)
	}
	if len(cols) == 0 {
		b = b.Select("*")
	} else {
		b = b.Select(cols...)
	}
	return b, nil
}
