package ondatra

import "fmt"

type JoinExpr interface {
	Expr
	SelectColumns() []string
	NewJoin(joinType string, table Table, alias, field, relatedField string) JoinExpr
	Alias() string
	RelatedTable(relatedTable string) JoinExpr
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
		joinType:     joinType,
		table:        table,
		alias:        alias,
		field:        field,
		relatedTable: b.table,
		relatedField: relatedField,
	}
}

func (b tableJoinBuilder) Alias() string {
	return ""
}

func (b tableJoinBuilder) RelatedTable(_ string) JoinExpr {
	return b
}

type aliasJoinBuilder struct {
	joinType     string
	table        Table
	alias        string
	field        string
	relatedTable string
	relatedField string
}

func (b aliasJoinBuilder) ToSQL() (string, []any, error) {
	return NewExpr(fmt.Sprintf(
		"%s JOIN %s as \"%s\" ON \"%s\".%s = \"%s\".\"%s\"",
		b.joinType, b.table.Name(), b.alias, b.alias, b.field, b.relatedTable, b.relatedField,
	)).ToSQL()
}

func (b aliasJoinBuilder) SelectColumns() []string {
	return b.table.ColumnsAlias(b.alias)
}

func (b aliasJoinBuilder) NewJoin(joinType string, table Table, alias, field, relatedField string) JoinExpr {
	alias = fmt.Sprintf("%s.%s", b.alias, alias)
	return &aliasJoinBuilder{
		joinType:     joinType,
		table:        table,
		alias:        alias,
		field:        field,
		relatedTable: b.alias,
		relatedField: relatedField,
	}
}

func (b aliasJoinBuilder) Alias() string {
	return b.alias
}

func (b aliasJoinBuilder) RelatedTable(relatedTable string) JoinExpr {
	b.relatedField = fmt.Sprintf("%s.%s", b.relatedTable, b.relatedField)
	b.relatedTable = relatedTable
	return b
}
