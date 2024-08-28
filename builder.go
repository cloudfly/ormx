package ormx

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	sb "github.com/huandu/go-sqlbuilder"
)

type Builder interface {
	Build() (string, []any)
}

// Build is same with builder.Build, but it will try to inject namespace(which defined in context) filter into where condition in sql
func Build(ctx context.Context, b Builder) (string, []any) {
	switch x := b.(type) {
	case *sb.UpdateBuilder:
		appendNamespaceFilter(ctx, &x.Cond)
	case *sb.SelectBuilder:
		appendNamespaceFilter(ctx, &x.Cond)
	case *sb.DeleteBuilder:
		appendNamespaceFilter(ctx, &x.Cond)
	}
	return b.Build()
}

func appendNamespaceFilter(ctx context.Context, cond *sb.Cond) *sb.Cond {
	if s := namespaceValueForInject(ctx); s != "" {
		// append namespace where condition into Cond
		cond.E(namespaceColumnName, s)
	}
	return cond
}

func namespaceValueForInject(ctx context.Context) string {
	v := ctx.Value(namespaceCtxKey{})
	if v == nil {
		// no namespace in context
		return ""
	}

	s, ok := v.(string)
	if !ok {
		// incorrect namespace value in context, ignore it
		return ""
	}

	if s == "" || s == "-" || s == "<nil>" {
		// empty namespace value in context, ignore it
		return ""
	}

	if shouldIgnoreNamespace(ctx) {
		// user-defined force ignore namespace in context
		return ""
	}
	return s
}

// WithNamespace add namespace info into context
func WithNamespace(ctx context.Context, namespace string) context.Context {
	return context.WithValue(ctx, namespaceCtxKey{}, namespace)
}

// IgnnoreNamespace force ormx ignore the namespace info in context, so that Build will not inject namespace filter in sql
func IgnoreNamespace(ctx context.Context) context.Context {
	return context.WithValue(ctx, ignoreNamespaceCtxKey{}, true)
}

func shouldIgnoreNamespace(ctx context.Context) bool {
	v := ctx.Value(ignoreNamespaceCtxKey{})
	if v == nil {
		return false
	}
	switch x := v.(type) {
	case bool:
		return x
	case string:
		x = strings.ToLower(x)
		return x == "1" || x == "true"
	case int, int64, uint, uint64, float64, float32:
		return x != 0
	}
	return false
}

type namespaceCtxKey struct{}
type ignoreNamespaceCtxKey struct{}

// WhereFromStruct generate where exprs from data(type of struct), the returned value can be used by builder.Where method
func WhereFromStruct(c *sb.Cond, data any, dst []string) []string {
	if data == nil {
		return []string{}
	}
	v := dereferencedValue(reflect.ValueOf(data))
	t := dereferencedType(reflect.TypeOf(data))
	if !v.IsValid() || v.IsZero() {
		return []string{}
	}
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		fieldType := t.Field(i)
		if field.IsNil() {
			continue
		}
		name, _ := colNameFromTag(fieldType)
		if name == "" {
			continue
		}
		fieldValue := dereferencedValue(field).Interface()
		dst = appendWhereExpr(c, dst, name, fieldValue, fieldType.Tag.Get("op"))
	}
	return dst
}

// WhereFromStruct generate where exprs from []KV, the returned value can be used by builder.Where method
func WhereFromKVs(c *sb.Cond, filter KVs, dst []string) []string {
	if filter == nil {
		return []string{}
	}
	for _, kv := range filter {
		colName := kv.Key
		fieldValue := kv.Value
		dst = appendWhereExpr(c, dst, colName, fieldValue, kv.Extra)
	}
	return dst
}

func WhereFromIDs(c *sb.Cond, idList []int64, dst []string) []string {
	t := reflect.TypeOf(idList)
	if t.Kind() == reflect.Slice {
		dst = append(dst, c.In(primaryKey, Any2Slice(idList)...))
	}
	return dst
}

func WhereFromID(c *sb.Cond, id int64, dst []string) []string {
	t := reflect.TypeOf(id)
	if t.Kind() == reflect.Slice {
		dst = append(dst, c.E(primaryKey, id))
	}
	return dst
}

func WhereFrom(c *sb.Cond, filter any, dst []string) []string {
	if kvs, ok := filter.(KVs); ok {
		return WhereFromKVs(c, kvs, dst)
	}
	t := dereferencedType(reflect.TypeOf(filter))
	if kind := t.Kind(); kind == reflect.Struct {
		return WhereFromStruct(c, filter, dst)
	} else if kind == reflect.Slice {
		dst = append(dst, c.In(primaryKey, Any2Slice(filter)...))
	} else {
		dst = append(dst, c.E(primaryKey, filter))
	}
	return dst
}

func appendWhereExpr(c *sb.Cond, dst []string, column string, value any, op string) []string {
	switch op {
	case "":
		if dereferencedType(reflect.TypeOf(value)).Kind() == reflect.Slice {
			dst = append(dst, c.In(column, Any2Slice(value)...))
		} else {
			dst = append(dst, c.E(column, value))
		}
	case "e":
		dst = append(dst, c.E(column, value))
	case "ne":
		dst = append(dst, c.NE(column, value))
	case "gt":
		dst = append(dst, c.GreaterThan(column, value))
	case "gte":
		dst = append(dst, c.GreaterEqualThan(column, value))
	case "lt":
		dst = append(dst, c.LessThan(column, value))
	case "lte":
		dst = append(dst, c.LessEqualThan(column, value))
	case "in":
		if values := Any2Slice(value); len(values) > 0 {
			dst = append(dst, c.In(column, Any2Slice(value)...))
		} else {
			dst = append(dst, c.IsNull(column))
		}
	case "notin":
		dst = append(dst, c.NotIn(column, Any2Slice(value)...))
	case "like":
		dst = append(dst, c.Like(column, value))
	case "notlike":
		dst = append(dst, c.NotLike(column, value))
	}
	return dst
}

// TableName auto recoganize the table name from data, it will auto prepend the tableNamePrefix which can be set by SetTableNamePrefix to the result.
//   - having Table() method, it will call d.Table() to get the table name
//   - type of struct, it will use the struct name, and snake case it
//   - type of string, return the name.
//   - type of other, return fmt.Sprintf("%s", d)
func TableName(d interface{}) string {
	if d == nil {
		return ""
	}
S:
	t, ok := d.(interface {
		Table() string
	})

	if ok {
		return t.Table()
	}

	var (
		vt   = dereferencedElemType(reflect.TypeOf(d))
		name string
	)

	switch vt.Kind() {
	case reflect.String:
		return d.(string)
	case reflect.Struct:
		structName := vt.Name()
		name = sb.SnakeCaseMapper(structName)
	case reflect.Slice:
		d = reflect.New(vt.Elem()).Interface()
		goto S
	default:
		name = fmt.Sprintf("%s", d)
	}

	if strings.HasPrefix(name, tableNamePrefix) {
		return name
	}

	return tableNamePrefix + name
}

// ColNamesWithTagOpt will column names from structure data, the type of d must be a struct, otherwise will return []string{}.
//
// ColNamesWithTagOpt will try to filter the filter the struct field which having <tag> specified in StructField.Tag if <tag> is not empty
func ColNamesWithTagOpt(d interface{}, tag string) []string {
	vt := reflect.TypeOf(d)
	if vt.Kind() == reflect.Ptr {
		vt = vt.Elem()
	}
	if vt.Kind() != reflect.Struct {
		return []string{}
	}
	table := TableName(d)
	var cols []string
	for i := 0; i < vt.NumField(); i++ {
		field := vt.Field(i)
		name, after := colNameFromTag(field)
		if name == "" {
			continue
		}
		if tag != "" {
			if opts := ParseOptionStr(after); opts != nil {
				if _, ok := opts[tag]; !ok {
					continue
				}
			}
		}
		cols = append(cols, table+"."+name)
	}
	return cols
}

func colNameFromTag(field reflect.StructField) (string, string) {
	if !field.IsExported() {
		return "", ""
	}
	switch field.Type.Kind() {
	case reflect.Func, reflect.Chan:
		return "", ""
	}
	name, after, _ := strings.Cut(field.Tag.Get(structTagName), ",")
	if name == "-" {
		return "", ""
	} else if name == "" {
		return field.Name, after
	}
	return name, after
}

func newCond() *sb.Cond {
	return &sb.Cond{
		Args: &sb.Args{},
	}
}
