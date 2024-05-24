package ondatra

import (
	"fmt"
	"strings"
)

type Column[T comparable] struct {
	Name    string
	Set     SetValue[T]
	EQ      Value[T]
	NEQ     Value[T]
	LT      Value[T]
	LTE     Value[T]
	GT      Value[T]
	GTE     Value[T]
	Like    Value[T]
	NotLike Value[T]
}

func NewColumn[T comparable](name string) Column[T] {
	return Column[T]{
		Name:    name,
		Set:     SetValue[T](fmt.Sprintf("%s = ?", name)),
		EQ:      Value[T](fmt.Sprintf("%s = ?", name)),
		NEQ:     Value[T](fmt.Sprintf("%s != ?", name)),
		LT:      Value[T](fmt.Sprintf("%s <= ?", name)),
		LTE:     Value[T](fmt.Sprintf("%s < ?", name)),
		GT:      Value[T](fmt.Sprintf("%s > ?", name)),
		GTE:     Value[T](fmt.Sprintf("%s >= ?", name)),
		Like:    Value[T](fmt.Sprintf("%s LIKE ?", name)),
		NotLike: Value[T](fmt.Sprintf("%s NOT LIKE ?", name)),
	}
}

func (c Column[T]) IN(value ...T) Expr {
	return NewExpr(
		fmt.Sprintf("%s IN (%s)", c.Name, strings.TrimRight(strings.Repeat("?,", len(value)), ",")),
		c.convertToArguments(value)...,
	)
}

func (c Column[T]) NIN(value ...T) Expr {
	return NewExpr(
		fmt.Sprintf("%s NOT IN (%s)", c.Name, strings.TrimRight(strings.Repeat("?,", len(value)), ",")),
		c.convertToArguments(value)...,
	)
}

func (c Column[T]) IsNull() Expr {
	return NewExpr(fmt.Sprintf("%s IS NULL", c.Name))
}

func (c Column[T]) IsNullValue(value bool) Expr {
	if value {
		return c.IsNull()
	}
	return nil
}

func (c Column[T]) IsNotNull() Expr {
	return NewExpr(fmt.Sprintf("%s IS NOT NULL", c.Name))
}

func (c Column[T]) IsNotNullValue(value bool) Expr {
	if value {
		return c.IsNotNull()
	}
	return nil
}

func (c Column[T]) IsNullPtr(value *bool) Expr {
	if value == nil {
		return nil
	}
	if *value {
		return c.IsNull()
	}
	return c.IsNotNull()
}

func (c Column[T]) convertToArguments(value []T) []any {
	args := make([]any, len(value))
	for i := range value {
		args[i] = value[i]
	}
	return args
}

type Value[T comparable] string

func (v Value[T]) Value(value T) Expr {
	return NewExpr(string(v), value)
}

func (v Value[T]) Ptr(ptr *T) Expr {
	if ptr == nil {
		return nil
	}
	return NewExpr(string(v), *ptr)
}

type SetValue[T comparable] string

func (v SetValue[T]) Value(value T) Expr {
	return NewExpr(string(v), value)
}

func (v SetValue[T]) Ptr(ptr *T) Expr {
	return NewExpr(string(v), *ptr)
}

func (v SetValue[T]) SetNull() Expr {
	return NewExpr(string(v), nil)
}

func OR(conditions ...Expr) Expr {
	var rawSQLs []string
	var args []any
	for i := range conditions {
		if conditions[i] == nil {
			continue
		}

		rawSQl, sqlArgs, err := conditions[i].ToSQL()
		if err != nil {
			return nil
		}

		rawSQLs = append(rawSQLs, rawSQl)
		args = append(args, sqlArgs...)
	}

	return NewExpr(fmt.Sprintf("(%s)", strings.Join(rawSQLs, " OR ")), args...)
}

func AND(conditions ...Expr) Expr {
	var rawSQLs []string
	var args []any
	for i := range conditions {
		if conditions[i] == nil {
			continue
		}

		rawSQl, sqlArgs, err := conditions[i].ToSQL()
		if err != nil {
			return nil
		}

		rawSQLs = append(rawSQLs, rawSQl)
		args = append(args, sqlArgs...)
	}

	return NewExpr(fmt.Sprintf("(%s)", strings.Join(rawSQLs, " AND ")), args...)
}
