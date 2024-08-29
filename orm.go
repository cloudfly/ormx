package ormx

import (
	"context"
	"fmt"

	"github.com/cloudfly/flagx"
	"github.com/cloudfly/ormx/cache"
)

var (
	tableNamePrefix     = flagx.NewString("database.table.prefix", "", "the table name's common prefix")
	structTagName       = "db"
	namespaceColumnName = flagx.NewString("database.table.namespace.column", "namespace", "the column name used to represent row's namespace")
	primaryKey          = flagx.NewString("database.table.primarykey", "id", "the primary id column name")
)

// Init the ormx, setting the sqlx.DB getter and common table name prefix
func Init(ctx context.Context, provider DBProvider) error {
	if err := Connect(ctx); err != nil {
		return err
	}
	if provider != nil {
		p = provider
	}
	return cache.Init()
}

// SetStructTagName set the tag name in Go Struct Tag, in which specify the ormx options, default is 'db'
func SetStructTagName(name string) {
	structTagName = name
}

// SetPrimaryKey set the primary column name, default is 'id'
func SetPrimaryKey(name string) {
	if name != "" {
		*primaryKey = name
	}
}

// SetNamespaceColumnName set the common namespace colunm name, default is 'namespace';
//
// ormx will auto inject namespace where condition into sql.// Set to empty string disable this feature
func SetNamespaceColumnName(name string) {
	*namespaceColumnName = name
}

type masterCtxKey struct{}

func isFromMaster(ctx context.Context) bool {
	return fmt.Sprintf("%v", ctx.Value(masterCtxKey{})) == "true"
}

// FromMaster force ormx execute sql on master instance when called by this context
func FromMaster(ctx context.Context) context.Context {
	return context.WithValue(ctx, masterCtxKey{}, "true")
}

// FromMaster force ormx execute sql on slave instance when called by this context
func FromSlave(ctx context.Context) context.Context {
	return context.WithValue(ctx, masterCtxKey{}, "false")
}

func convertValueByDBType(v any, tag string) any {
	switch tag {
	case "timestamp":
		if t, ok := Any2Time(v); ok {
			return t
		}
	}
	return v
}
