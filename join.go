package ondatra

import "fmt"

type JoinBuilder interface {
	Expr
	NewJoin(joinType, table, alias, field, relatedField string) JoinBuilder
}

func NewJoinBuilder(table string) JoinBuilder {
	return &tableJoinBuilder{
		table: table,
	}
}

type tableJoinBuilder struct {
	table string
}

func (b tableJoinBuilder) ToSQL() (string, []any, error) {
	return "", nil, nil
}

func (b tableJoinBuilder) NewJoin(joinType, table, alias, field, relatedField string) JoinBuilder {
	return &aliasJoinBuilder{
		Expr: NewExpr(fmt.Sprintf(
			"%s JOIN %s as \"%s\" ON \"%s\".%s = \"%s\".%s",
			joinType, table, alias, alias, field, b.table, relatedField,
		)),
		alias: alias,
	}
}

type aliasJoinBuilder struct {
	Expr
	alias string
}

func (b aliasJoinBuilder) NewJoin(joinType, table, alias, field, relatedField string) JoinBuilder {
	alias = fmt.Sprintf("%s.%s", b.alias, alias)
	return &aliasJoinBuilder{
		Expr: NewExpr(fmt.Sprintf(
			"%s JOIN %s as \"%s\" ON \"%s\".%s = \"%s\".%s",
			joinType, table, alias, alias, field, b.alias, relatedField,
		)),
		alias: alias,
	}
}
