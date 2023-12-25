package ondatra

type Clause interface {
	Apply(b Builder) Builder
}

type prefixClause struct {
	rawSQL string
	args   []any
}

func (c prefixClause) Apply(b Builder) Builder {
	return b.Prefix(c.rawSQL, c.args...)
}

func Prefix(rawSQL string, args ...any) Clause {
	return prefixClause{
		rawSQL: rawSQL,
		args:   args,
	}
}

type selectColumnsClause struct {
	columns []string
}

func (c selectColumnsClause) Apply(b Builder) Builder {
	return b.SelectColumns(c.columns...)
}

func SelectColumns(columns ...string) Clause {
	return selectColumnsClause{
		columns: columns,
	}
}

type selectColumnClause struct {
	rawSQL string
	args   []any
}

func (c selectColumnClause) Apply(b Builder) Builder {
	return b.SelectColumn(c.rawSQL, c.args...)
}

func SelectColumn(rawSQL string, args ...any) Clause {
	return selectColumnClause{
		rawSQL: rawSQL,
		args:   args,
	}
}

type optionsClause struct {
	options []string
}

func (c optionsClause) Apply(b Builder) Builder {
	return b.Options(c.options...)
}

func Options(options ...string) Clause {
	return optionsClause{
		options: options,
	}
}

type distinctClause struct{}

func (c distinctClause) Apply(b Builder) Builder {
	return b.Distinct()
}

func Distinct() Clause {
	return distinctClause{}
}

type columnsClause struct {
	columns []string
}

func (c columnsClause) Apply(b Builder) Builder {
	return b.Columns(c.columns...)
}

func Columns(columns ...string) Clause {
	return columnsClause{
		columns: columns,
	}
}

type setClause struct {
	column string
	value  any
}

func (c setClause) Apply(b Builder) Builder {
	return b.Set(c.column, c.value)
}

func Set(column string, value any) Clause {
	return setClause{
		column: column,
		value:  value,
	}
}

type setExprClause struct {
	expr []Expr
}

func (c setExprClause) Apply(b Builder) Builder {
	return b.SetExpr(c.expr...)
}

func SetExpr(expr ...Expr) Clause {
	return setExprClause{
		expr: expr,
	}
}

type joinClause struct {
	joinType string
	join     string
	args     []any
}

func (c joinClause) Apply(b Builder) Builder {
	return b.Join(c.joinType, c.join)
}

func Join(joinType, join string, args ...any) Clause {
	return joinClause{
		joinType: joinType,
		join:     join,
		args:     args,
	}
}

type joinRawClause struct {
	rawSQL string
	args   []any
}

func (c joinRawClause) Apply(b Builder) Builder {
	return b.JoinRaw(c.rawSQL, c.args...)
}

func JoinRaw(rawSQL string, args ...any) Clause {
	return joinRawClause{
		rawSQL: rawSQL,
		args:   args,
	}
}

type joinExprClause struct {
	expr []Expr
}

func (c joinExprClause) Apply(b Builder) Builder {
	return b.JoinExpr(c.expr...)
}

func JoinExpr(expr ...Expr) Clause {
	return joinExprClause{
		expr: expr,
	}
}

type whereClause struct {
	rawSQL string
	args   []any
}

func (c whereClause) Apply(b Builder) Builder {
	return b.Where(c.rawSQL, c.args...)
}

func Where(rawSQL string, args ...any) Clause {
	return whereClause{
		rawSQL: rawSQL,
		args:   args,
	}
}

type whereExprClause struct {
	expr []Expr
}

func (c whereExprClause) Apply(b Builder) Builder {
	return b.WhereExpr(c.expr...)
}

func WhereExpr(expr ...Expr) Clause {
	return whereExprClause{
		expr: expr,
	}
}

type groupByClause struct {
	groupBys []string
}

func (c groupByClause) Apply(b Builder) Builder {
	return b.GroupBy(c.groupBys...)
}

func GroupBy(groupBys ...string) Clause {
	return groupByClause{
		groupBys: groupBys,
	}
}

type havingClause struct {
	rawSQL string
	args   []any
}

func (c havingClause) Apply(b Builder) Builder {
	return b.Having(c.rawSQL, c.args...)
}

func Having(rawSQL string, args ...any) Clause {
	return havingClause{
		rawSQL: rawSQL,
		args:   args,
	}
}

type orderByClause struct {
	orderBy []string
}

func (c orderByClause) Apply(b Builder) Builder {
	return b.OrderBy(c.orderBy...)
}

func OrderBy(orderBy ...string) Clause {
	return orderByClause{
		orderBy: orderBy,
	}
}

type orderByArgsClause struct {
	rawSQL string
	args   []any
}

func (c orderByArgsClause) Apply(b Builder) Builder {
	return b.OrderByArgs(c.rawSQL, c.args...)
}

func OrderByArgs(rawSQL string, args ...any) Clause {
	return orderByArgsClause{
		rawSQL: rawSQL,
		args:   args,
	}
}

type limitClause struct {
	limit int64
}

func (c limitClause) Apply(b Builder) Builder {
	return b.Limit(c.limit)
}

func Limit(limit int64) Clause {
	return limitClause{
		limit: limit,
	}
}

type offsetClause struct {
	offset int64
}

func (c offsetClause) Apply(b Builder) Builder {
	return b.Offset(c.offset)
}

func Offset(offset int64) Clause {
	return offsetClause{
		offset: offset,
	}
}

type limitOffsetClause struct {
	limit  int64
	offset int64
}

func (c limitOffsetClause) Apply(b Builder) Builder {
	return b.LimitOffset(c.limit, c.offset)
}

func LimitOffset(limit, offset int64) Clause {
	return limitOffsetClause{
		limit:  limit,
		offset: offset,
	}
}

type suffixClause struct {
	rawSQL string
	args   []any
}

func (c suffixClause) Apply(b Builder) Builder {
	return b.Suffix(c.rawSQL, c.args...)
}

func Suffix(rawSQL string, args ...any) Clause {
	return suffixClause{
		rawSQL: rawSQL,
		args:   args,
	}
}
