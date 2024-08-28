package ormx

import (
	"context"
	"fmt"

	"github.com/cloudfly/ormx/cache"
)

var (
	tableNamePrefix     = ""
	structTagName       = "db"
	namespaceColumnName = "namespace"
	primaryKey          = "id"
)

// Init the ormx, setting the sqlx.DB getter and common table name prefix
func Init(provider DBProvider, tablePrefix string) error {
	p = provider
	tableNamePrefix = tablePrefix
	return cache.Init()
}

// SetStructTagName set the tag name in Go Struct Tag, in which specify the ormx options, default is 'db'
func SetStructTagName(name string) {
	structTagName = name
}

// SetPrimaryKey set the primary column name, default is 'id'
func SetPrimaryKey(name string) {
	if name != "" {
		primaryKey = name
	}
}

// SetNamespaceColumnName set the common namespace colunm name, default is 'namespace';
//
// ormx will auto inject namespace where condition into sql.// Set to empty string disable this feature
func SetNamespaceColumnName(name string) {
	namespaceColumnName = name
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
