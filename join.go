package ondatra

import "fmt"

type JoinExpr interface {
	Expr
	SelectColumns() []string
	NewJoin(joinType string, table Table, alias, field, relatedField string) JoinExpr
}

func NewJoinBuilder(table string) JoinExpr {
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

func (b tableJoinBuilder) SelectColumns() []string {
	return nil
}

func (b tableJoinBuilder) NewJoin(joinType string, table Table, alias, field, relatedField string) JoinExpr {
	return &aliasJoinBuilder{
		Expr: NewExpr(fmt.Sprintf(
			"%s JOIN %s as \"%s\" ON \"%s\".%s = \"%s\".%s",
			joinType, table.Name(), alias, alias, field, b.table, relatedField,
		)),
		alias:         alias,
		selectColumns: table.ColumnsAlias(alias),
	}
}

type aliasJoinBuilder struct {
	Expr
	alias         string
	selectColumns []string
}

func (b aliasJoinBuilder) SelectColumns() []string {
	return b.selectColumns
}

func (b aliasJoinBuilder) NewJoin(joinType string, table Table, alias, field, relatedField string) JoinExpr {
	alias = fmt.Sprintf("%s.%s", b.alias, alias)
	return &aliasJoinBuilder{
		Expr: NewExpr(fmt.Sprintf(
			"%s JOIN %s as \"%s\" ON \"%s\".%s = \"%s\".%s",
			joinType, table.Name(), alias, alias, field, b.alias, relatedField,
		)),
		alias:         alias,
		selectColumns: table.ColumnsAlias(alias),
	}
}
