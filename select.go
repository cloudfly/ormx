package ormx

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/cloudfly/ormx/cache"
	sb "github.com/huandu/go-sqlbuilder"
	"github.com/rs/zerolog"
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
	b = b.Where(WhereFrom(&b.Cond, id, nil)...)

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
		zerolog.Ctx(ctx).Warn().Err(err).Str("query", statement).Any("args", args).Msg("Failed to marshal data for cacheing")
		// 忽略序列化错误，顶多就是无法cache，无关紧要
		return nil
	}
	cache.Set(time.Second*10, table, id, content)
	return nil
}

// GetWhere 使用自定义条件跟新数据
func GetWhere(ctx context.Context, dst interface{}, table string, fields []string, filter KVs) error {
	if table == "" {
		table = TableName(dst)
	}
	builder, err := NewSelectBuilderFromStruct(table, dst)
	if err != nil {
		return fmt.Errorf("new select builder error: %w", err)
	}
	if len(fields) == 0 {
		builder = builder.Select(fields...)
	}
	builder = builder.Where(WhereFrom(&builder.Cond, filter, nil)...)
	sql, args := Build(ctx, builder)
	return Get(ctx, dst, sql, args...)
}

// GetWhere 使用自定义条件跟新数据
func SelectWhere(ctx context.Context, dst interface{}, table string, fields []string, filter KVs, sort []string, page, pageSize int) error {
	if table == "" {
		table = TableName(dst)
	}
	builder, err := NewSelectBuilderFromStruct(table, dst)
	if err != nil {
		return fmt.Errorf("new select builder error: %w", err)
	}
	if len(fields) == 0 {
		builder = builder.Select(fields...)
	}
	builder = builder.Where(WhereFrom(&builder.Cond, filter, nil)...)

	if len(sort) > 0 {
		orderByCols := make([]string, 0, 8)
		for _, col := range sort {
			if col == "" {
				continue
			}
			if col[0] == '-' {
				orderByCols = append(orderByCols, strings.TrimLeft(col, "-")+" DESC")
			} else {
				orderByCols = append(orderByCols, col+" ASC")
			}
		}
		builder = builder.OrderBy(orderByCols...)
	}
	if page > 0 && pageSize > 0 {
		builder = builder.Limit(pageSize).Offset((page - 1) * pageSize)
	}

	sql, args := Build(ctx, builder)

	return Select(ctx, dst, sql, args...)
}

// Count select the count of rows in table which match the filter condition
func Count(ctx context.Context, table string, filter any) (int64, error) {
	total := sql.NullInt64{}
	b := sb.NewSelectBuilder().Select("COUNT(1) as total").From(table)
	b = b.Where(WhereFrom(&b.Cond, filter, nil)...)

	sql, args := Build(ctx, b)
	err := Get(ctx, &total, sql, args...)
	if IsNotFound(err) {
		err = nil
	}
	return total.Int64, err
}

// Count select the count of rows in table which match the filter condition
func CountBy(ctx context.Context, table string, filter any, group []string) ([]M, error) {
	cols := []string{"COUNT(1) as total"}
	if len(group) > 0 {
		cols = append(cols, group...)
	}
	b := sb.NewSelectBuilder().Select(cols...).From(table)
	b = b.Where(WhereFrom(&b.Cond, filter, nil)...)

	if len(group) > 0 {
		b = b.GroupBy(group...)
	}

	data := []M{}
	sql, args := Build(ctx, b)
	err := Select(ctx, &data, sql, args...)
	if IsNotFound(err) {
		err = nil
	}
	return data, err
}

// Distinct fetch distinct values of the column in table
func Distinct(ctx context.Context, table, column string, filter KVs) ([]any, error) {
	builder := sb.NewSelectBuilder().From(table)
	builder = builder.Select(fmt.Sprintf("DISTINCT(%s) as %s", sb.Escape(column), sb.Escape(column)))
	conds := WhereFromKVs(&builder.Cond, filter, nil)
	builder = builder.Where(conds...)
	sql, args := Build(ctx, builder)

	data := []any{}
	if err := Select(ctx, &data, sql, args...); err != nil {
		return nil, fmt.Errorf("select error: %w", err)
	}
	return data, nil
}

// Exist return true if the at least one row found in table by using where condition
func Exist(ctx context.Context, table string, filter any) (bool, error) {
	n := sql.NullInt64{}
	b := sb.NewSelectBuilder().Select("1").From(table).Limit(1)
	b = b.Where(WhereFrom(&b.Cond, filter, nil)...)
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
