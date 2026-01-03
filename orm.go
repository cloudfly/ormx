package ormx

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/cloudfly/flagx"
	"github.com/cloudfly/ormx/cache"
	"github.com/jmoiron/sqlx"
)

var (
	tableNamePrefix = flagx.NewString("db.table.prefix", "", "the table name's common prefix")
	primaryKey      = flagx.NewString("db.table.primarykey", "id", "the primary id column name")

	gopt *Option = &Option{}
)

// Init the ormx, setting the sqlx.DB getter and common table name prefix
func Init(ctx context.Context, opt *Option) error {
	if err := Connect(ctx); err != nil {
		return fmt.Errorf("connect db error: %w", err)
	}
	if opt == nil {
		opt = &Option{}
	}
	newOpt := opt.Copy()
	if newOpt.tagName == "" {
		newOpt.tagName = "db"
	}
	if newOpt.tablePrefix == "" {
		newOpt.tablePrefix = *tableNamePrefix
	}
	if newOpt.primaryKey == "" {
		newOpt.primaryKey = *primaryKey
	}
	gopt = newOpt

	return cache.Init()
}

// SetGlobalOption set option for all the sql execution
func SetGlobalOption(opt *Option) {
	gopt = opt.Copy()
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

// Option for select or insert data
type Option struct {
	table               string
	tablePrefix         string
	namespace           string
	ignoreNamespace     bool
	fields              []string
	sorts               []string
	page                int
	pageSize            int
	primaryKey          string
	tagName             string
	namespaceColumnName string
	fromMaster          bool
	tx                  *sqlx.Tx
	txLevel             sql.IsolationLevel
}

type optionCtxKey struct{}

// NewOptionCtx return a new option for execute sql
func NewOption() *Option {
	return gopt.Copy()
}

// CtxOption try to get option from context, if not found, return NewOption()
func CtxOption(ctx context.Context) *Option {
	v := ctx.Value(optionCtxKey{})
	if v != nil {
		return v.(*Option)
	}
	return gopt.Copy()
}

func (opt *Option) Copy() *Option {
	if opt == nil {
		return nil
	}
	newOpt := *opt
	newOpt.fields = append([]string{}, opt.fields...)
	newOpt.sorts = append([]string{}, opt.sorts...)

	return &newOpt
}

func (opt *Option) Table(value any) *Option {
	switch data := value.(type) {
	case string:
		opt.table = data
	case interface{ Table() string }:
		opt.table = data.Table()
	}
	return opt
}

func (opt *Option) Namespace(value string) *Option {
	opt.namespace = value
	return opt
}

func (opt *Option) IgnoreNamespace() *Option {
	opt.ignoreNamespace = true
	return opt
}

func (opt *Option) Fields(value ...string) *Option {
	opt.fields = value
	return opt
}

func (opt *Option) Sorts(value ...string) *Option {
	opt.sorts = value
	return opt
}

func (opt *Option) Page(page, pageSize int) *Option {
	opt.page, opt.pageSize = page, pageSize
	return opt
}

func (opt *Option) PrimaryKey(value string) *Option {
	opt.primaryKey = value
	return opt
}

func (opt *Option) TagName(value string) *Option {
	opt.tagName = value
	return opt
}

func (opt *Option) NamespaceColumnName(value string) *Option {
	opt.namespaceColumnName = value
	return opt
}

func (opt *Option) TablePrefix(value string) *Option {
	opt.tablePrefix = value
	return opt
}

func (opt *Option) Tx(tx *sqlx.Tx) *Option {
	opt.tx = tx
	return opt
}

func (opt *Option) Isolation(level sql.IsolationLevel) *Option {
	opt.txLevel = level
	return opt
}

func (opt *Option) FromMaster() *Option {
	opt.fromMaster = true
	return opt
}

// With put the Option into context by Context.WithValue
func (opt *Option) With(ctx context.Context) context.Context {
	return context.WithValue(ctx, optionCtxKey{}, opt)
}
