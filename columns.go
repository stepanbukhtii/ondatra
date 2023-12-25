package ondatra

import (
	"fmt"
	"strings"
)

type Column[T comparable] struct {
	Name    string
	Set     Value[T]
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
		Set:     Value[T](fmt.Sprintf("%s = ?", name)),
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

func (c Column[T]) IN(value ...any) Expr {
	return NewExpr(
		fmt.Sprintf("%s IN (%s)", c.Name, strings.TrimRight(strings.Repeat("?,", len(value)), ",")),
		value...,
	)
}

func (c Column[T]) NIN(value ...any) Expr {
	return NewExpr(
		fmt.Sprintf("%s NOT IN (%s)", c.Name, strings.TrimRight(strings.Repeat("?,", len(value)), ",")),
		value...,
	)
}

func (c Column[T]) IsNull() Expr {
	return NewExpr(fmt.Sprintf("%s IS NULL", c.Name))
}

func (c Column[T]) IsNotNull() Expr {
	return NewExpr(fmt.Sprintf("%s IS NOT NULL", c.Name))
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
