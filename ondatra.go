package ondatra

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"github.com/jmoiron/sqlx"
	"log"
	"reflect"
	"slices"
	"strings"
)

const (
	CommandSelect = "SELECT"
	CommandInsert = "INSERT"
	CommandUpdate = "UPDATE"
	CommandDelete = "DELETE"

	JoinInner = "INNER"
	JoinLeft  = "LEFT"
	JoinRight = "RIGHT"
	JoinFull  = "FULL"

	ColumnCreatedAt = "created_at"
	ColumnUpdatedAt = "updated_at"

	modelTagColumn     = "column"
	modelTagDefault    = "default"
	modelTagPrimaryKey = "pk"
)

var DebugMode = false

type Builder struct {
	writerConn        Connection
	readerConn        Connection
	placeholderFormat PlaceholderFormat

	prefixes         []Expr   // for all
	command          string   // for all
	options          []string // for all
	table            Expr     // for all
	selectExpr       []Expr   // only for select
	columns          []string // only for insert or update
	insertValues     [][]any  // only for insert
	returningColumns []string // only for insert or update
	returningDest    []any    // only for insert or update
	updateValues     []Expr   // only for update
	joins            []Expr   // only for select
	whereExpr        []Expr   // for all
	groupBys         []string // only for select
	havingParts      []Expr   // only for select
	orderByParts     []Expr   // only for select
	limit            int64    // only for select
	offset           int64    // only for select
	suffixes         []Expr   // for all
}

// NewEmptyBuilder create empty builder only for ToSQL()
func NewEmptyBuilder() Builder {
	return Builder{}
}

func NewBuilder(db *sqlx.DB) Builder {
	return Builder{
		writerConn: NewDB(db),
	}
}

func NewBuilderWriterReader(writerDB *sqlx.DB, readerDB *sqlx.DB) Builder {
	return Builder{
		writerConn: NewDB(writerDB),
		readerConn: NewDB(readerDB),
	}
}

func NewBuilderTx(tx *sqlx.Tx) Builder {
	return Builder{
		writerConn: NewTx(tx),
	}
}

func (b Builder) New() Builder {
	return Builder{
		writerConn: b.writerConn,
		readerConn: b.readerConn,
	}
}

func (b Builder) RunInTransaction(ctx context.Context, exec func(Builder) error) error {
	tx, err := b.writerConn.BeginTx(ctx)
	if err != nil {
		return err
	}

	if DebugMode {
		log.Println("Begin transaction")
	}

	defer func() {
		recoveredFrom := recover()
		if recoveredFrom != nil {
			if DebugMode {
				fmt.Println("Rollback transaction")
			}
			_ = tx.Rollback()

			switch v := recoveredFrom.(type) {
			case error:
				err = v
			case string:
				err = errors.New(v)
			default:
				err = fmt.Errorf("unknown panic: %v", recoveredFrom)
			}
		}
	}()

	if err = exec(NewBuilderTx(tx)); err != nil {
		if DebugMode {
			log.Println("Rollback transaction")
		}
		_ = tx.Rollback()
		return err
	}

	if DebugMode {
		log.Println("Commit transaction")
	}
	return tx.Commit()
}

func (b Builder) Clauses(clauses ...Clause) Builder {
	for i := range clauses {
		b = clauses[i].Apply(b)
	}
	return b
}

func (b Builder) Prefix(rawSQL string, args ...any) Builder {
	b.prefixes = append(b.prefixes, NewExpr(rawSQL, args...))
	return b
}

func (b Builder) Insert() Builder {
	b.command = CommandInsert
	return b
}

func (b Builder) Select(columns ...string) Builder {
	b.command = CommandSelect
	return b.SelectColumns(columns...)
}

func (b Builder) Update() Builder {
	b.command = CommandUpdate
	return b
}

func (b Builder) Delete() Builder {
	b.command = CommandDelete
	return b
}

func (b Builder) Command(command string) Builder {
	b.command = command
	return b
}

func (b Builder) SelectColumns(columns ...string) Builder {
	for _, column := range columns {
		b.selectExpr = append(b.selectExpr, NewExpr(column))
	}
	return b
}

func (b Builder) SelectColumn(rawSQL string, args ...any) Builder {
	b.selectExpr = append(b.selectExpr, NewExpr(rawSQL, args...))
	return b
}

func (b Builder) Options(options ...string) Builder {
	b.options = append(b.options, options...)
	return b
}

func (b Builder) Distinct() Builder {
	b.options = append(b.options, "DISTINCT")
	return b
}

// Columns use for insert columns
func (b Builder) Columns(columns ...string) Builder {
	b.columns = append(b.columns, columns...)
	return b
}

// Values use for insert many values array
func (b Builder) Values(values ...any) Builder {
	b.insertValues = append(b.insertValues, values)
	return b
}

// Set use for update
func (b Builder) Set(column string, value any) Builder {
	b.updateValues = append(b.updateValues, NewExpr(fmt.Sprintf("%s = ?", column), value))
	return b
}

// SetMap use for update
func (b Builder) SetMap(clauses map[string]any) Builder {
	for column, value := range clauses {
		b.updateValues = append(b.updateValues, NewExpr(fmt.Sprintf("%s = ?", column), value))
	}
	return b
}

// SetExpr use for update
func (b Builder) SetExpr(expr ...Expr) Builder {
	for i := range expr {
		if expr[i] != nil {
			b.updateValues = append(b.updateValues, expr[i])
		}
	}
	return b
}

// StructColumns set columns for insert or update from struct with db tags
func (b Builder) StructColumns(object any) Builder {
	switch b.command {
	case CommandInsert:
		return b.insertStructColumns(object)
	case CommandUpdate:
		return b.updateStructColumns(object)
	default:
		return b
	}
}

func (b Builder) Table(table string) Builder {
	b.table = NewExpr(table)
	return b
}

func (b Builder) Into(table string) Builder {
	b.table = NewExpr(table)
	return b
}

func (b Builder) From(table string) Builder {
	b.table = NewExpr(table)
	return b
}

func (b Builder) FromSelect(from Expr, alias string) Builder {
	b.table = NewExpr("(?) AS "+alias, from)
	return b
}

func (b Builder) JoinRaw(rawSQL string, args ...any) Builder {
	b.joins = append(b.joins, NewExpr(rawSQL, args...))
	return b
}

func (b Builder) Join(joinType, join string, args ...any) Builder {
	return b.JoinRaw(fmt.Sprintf("%s JOIN %s", joinType, join), args...)
}

func (b Builder) JoinExpr(expr ...JoinExpr) Builder {
	for i := range expr {
		if expr[i] != nil {
			b.joins = append(b.joins, expr[i])
			columns := expr[i].SelectColumns()
			for _, column := range columns {
				b.selectExpr = append(b.selectExpr, NewExpr(column))
			}
		}
	}
	return b
}

func (b Builder) Where(rawSQL string, args ...any) Builder {
	if rawSQL != "" {
		b.whereExpr = append(b.whereExpr, NewExpr(rawSQL, args...))
	}
	return b
}

func (b Builder) WhereExpr(expr ...Expr) Builder {
	for i := range expr {
		if expr[i] != nil {
			b.whereExpr = append(b.whereExpr, expr[i])
		}
	}
	return b
}

func (b Builder) GroupBy(groupBys ...string) Builder {
	b.groupBys = append(b.groupBys, groupBys...)
	return b
}

func (b Builder) Having(rawSQL string, args ...any) Builder {
	b.havingParts = append(b.havingParts, NewExpr(rawSQL, args...))
	return b
}

func (b Builder) OrderBy(orderBys ...string) Builder {
	for _, orderBy := range orderBys {
		b.orderByParts = append(b.orderByParts, NewExpr(orderBy))
	}
	return b
}

func (b Builder) OrderByArgs(rawSQL string, args ...any) Builder {
	b.orderByParts = append(b.orderByParts, NewExpr(rawSQL, args...))
	return b
}

func (b Builder) LimitOffset(limit, offset int64) Builder {
	b.limit = limit
	b.offset = offset
	return b
}

func (b Builder) Limit(limit int64) Builder {
	b.limit = limit
	return b
}

func (b Builder) Offset(offset int64) Builder {
	b.offset = offset
	return b
}

func (b Builder) Suffix(rawSQL string, args ...any) Builder {
	b.suffixes = append(b.suffixes, NewExpr(rawSQL, args...))
	return b
}

func (b Builder) PlaceholderFormat(placeholderFormat PlaceholderFormat) Builder {
	b.placeholderFormat = placeholderFormat
	return b
}

func (b Builder) ToSQL() (string, []any, error) {
	var err error
	var args []any
	var buffer strings.Builder

	if len(b.prefixes) > 0 {
		if args, err = writeExprs(b.prefixes, &buffer, " ", args); err != nil {
			return "", nil, err
		}
		buffer.WriteString(" ")
	}

	buffer.WriteString(b.command)
	buffer.WriteString(" ")

	if len(b.options) > 0 {
		buffer.WriteString(strings.Join(b.options, " "))
		buffer.WriteString(" ")
	}

	switch b.command {
	case CommandSelect:
		if len(b.selectExpr) == 0 {
			return "", nil, NotSetColumns
		}

		if args, err = writeExprs(b.selectExpr, &buffer, ", ", args); err != nil {
			return "", nil, err
		}
		buffer.WriteString(" ")

		if b.table != nil {
			buffer.WriteString("FROM ")
			if args, err = writeExpr(b.table, &buffer, args); err != nil {
				return "", nil, err
			}
			buffer.WriteString(" ")
		}
	case CommandInsert:
		if len(b.insertValues) == 0 {
			return "", nil, NotSetValues
		}

		if b.table != nil {
			buffer.WriteString("INTO ")
			if args, err = writeExpr(b.table, &buffer, args); err != nil {
				return "", nil, err
			}
			buffer.WriteString(" ")
		}

		if len(b.columns) > 0 {
			buffer.WriteString(fmt.Sprintf("(%s) ", strings.Join(b.columns, ", ")))
		}

		buffer.WriteString("VALUES ")

		valuesStrings := make([]string, len(b.insertValues))
		for i, value := range b.insertValues {
			valueStrings := make([]string, len(value))
			for j, val := range value {
				if vs, ok := val.(Expr); ok {
					vsql, vargs, err := vs.ToSQL()
					if err != nil {
						return "", nil, err
					}
					valueStrings[j] = vsql
					args = append(args, vargs...)
				} else {
					valueStrings[j] = "?"
					args = append(args, val)
				}
			}
			valuesStrings[i] = "(" + strings.Join(valueStrings, ",") + ")"
		}
		buffer.WriteString(strings.Join(valuesStrings, ","))
		buffer.WriteString(" ")

		if len(b.returningColumns) > 0 {
			buffer.WriteString(fmt.Sprintf(" RETURNING %s", strings.Join(b.returningColumns, ", ")))
		}
	case CommandUpdate:
		if len(b.updateValues) == 0 {
			return "", nil, NotSetValues
		}
		if b.table != nil {
			if args, err = writeExpr(b.table, &buffer, args); err != nil {
				return "", nil, err
			}
		}

		buffer.WriteString(" SET ")

		if args, err = writeExprs(b.updateValues, &buffer, ", ", args); err != nil {
			return "", nil, err
		}
		buffer.WriteString(" ")

		if len(b.returningColumns) > 0 {
			buffer.WriteString(fmt.Sprintf(" RETURNING %s", strings.Join(b.returningColumns, ", ")))
		}
	case CommandDelete:
		if b.table != nil {
			buffer.WriteString("FROM ")
			if args, err = writeExpr(b.table, &buffer, args); err != nil {
				return "", nil, err
			}
			buffer.WriteString(" ")
		}
	}

	if len(b.joins) > 0 {
		if args, err = writeExprs(b.joins, &buffer, " ", args); err != nil {
			return "", nil, err
		}
		buffer.WriteString(" ")
	}

	if len(b.whereExpr) > 0 {
		buffer.WriteString("WHERE ")
		if args, err = writeExprs(b.whereExpr, &buffer, " AND ", args); err != nil {
			return "", nil, err
		}
		buffer.WriteString(" ")
	}

	if len(b.groupBys) > 0 {
		buffer.WriteString(fmt.Sprintf("GROUP BY %s ", strings.Join(b.groupBys, ", ")))
	}

	if len(b.havingParts) > 0 {
		buffer.WriteString("HAVING ")
		if args, err = writeExprs(b.havingParts, &buffer, " AND ", args); err != nil {
			return "", nil, err
		}
		buffer.WriteString(" ")
	}

	if len(b.orderByParts) > 0 {
		buffer.WriteString("ORDER BY ")
		if args, err = writeExprs(b.orderByParts, &buffer, ", ", args); err != nil {
			return "", nil, err
		}
		buffer.WriteString(" ")
	}

	if b.limit > 0 {
		buffer.WriteString(fmt.Sprintf("LIMIT %d ", b.limit))
	}

	if b.offset > 0 {
		buffer.WriteString(fmt.Sprintf("OFFSET %d ", b.offset))
	}

	if len(b.suffixes) > 0 {
		if args, err = writeExprs(b.suffixes, &buffer, " ", args); err != nil {
			return "", nil, err
		}
	}

	sqlString := buffer.String()
	if b.placeholderFormat != nil {
		sqlString = b.placeholderFormat.ReplacePlaceholders(sqlString)
	}

	return strings.TrimSpace(sqlString), args, nil
}

func (b Builder) ToQueryWithArgs() (string, []any, error) {
	if b.writerConn == nil && b.readerConn == nil {
		return "", nil, SqlDBNotSet
	}

	query, args, err := b.ToSQL()
	if err != nil {
		return "", nil, err
	}
	query = b.conn().Rebind(query)

	if DebugMode {
		log.Println("Query:", query, "Arguments:", args)
	}

	return query, args, nil
}

func (b Builder) Get(dest any) error {
	query, args, err := b.ToQueryWithArgs()
	if err != nil {
		return err
	}
	return b.conn().Get(dest, query, args...)
}

func (b Builder) GetContext(ctx context.Context, dest any) error {
	query, args, err := b.ToQueryWithArgs()
	if err != nil {
		return err
	}
	return b.conn().GetContext(ctx, dest, query, args...)
}

func (b Builder) GetAll(dest any) error {
	query, args, err := b.ToQueryWithArgs()
	if err != nil {
		return err
	}
	return b.conn().Select(dest, query, args...)
}

func (b Builder) GetAllContext(ctx context.Context, dest any) error {
	query, args, err := b.ToQueryWithArgs()
	if err != nil {
		return err
	}
	return b.conn().SelectContext(ctx, dest, query, args...)
}

func (b Builder) Exec() (sql.Result, error) {
	query, args, err := b.ToQueryWithArgs()
	if err != nil {
		return nil, err
	}
	return b.conn().Exec(query, args...)
}

func (b Builder) ExecContext(ctx context.Context) (sql.Result, error) {
	query, args, err := b.ToQueryWithArgs()
	if err != nil {
		return nil, err
	}
	return b.conn().ExecContext(ctx, query, args...)
}

func (b Builder) ExecRaw(query string, args ...any) (sql.Result, error) {
	return b.conn().Exec(query, args...)
}

func (b Builder) Query() (*sql.Rows, error) {
	query, args, err := b.ToQueryWithArgs()
	if err != nil {
		return nil, err
	}
	return b.conn().Query(query, args...)
}

func (b Builder) QueryContext(ctx context.Context) (*sql.Rows, error) {
	query, args, err := b.ToQueryWithArgs()
	if err != nil {
		return nil, err
	}
	return b.conn().QueryContext(ctx, query, args...)
}

func (b Builder) QueryRow() (*sql.Row, error) {
	query, args, err := b.ToQueryWithArgs()
	if err != nil {
		return nil, err
	}
	return b.conn().QueryRow(query, args...), nil
}

func (b Builder) QueryRowContext(ctx context.Context) (*sql.Row, error) {
	query, args, err := b.ToQueryWithArgs()
	if err != nil {
		return nil, err
	}
	return b.conn().QueryRowContext(ctx, query, args...), nil
}

func (b Builder) ExecReturning() error {
	if len(b.returningColumns) == 0 {
		if _, err := b.Exec(); err != nil {
			return err
		}
		return nil
	}

	query, args, err := b.ToQueryWithArgs()
	if err != nil {
		return err
	}

	return b.conn().QueryRow(query, args...).Scan(b.returningDest...)
}

func (b Builder) ExecReturningContext(ctx context.Context) error {
	if len(b.returningColumns) == 0 {
		if _, err := b.ExecContext(ctx); err != nil {
			return err
		}
		return nil
	}

	query, args, err := b.ToQueryWithArgs()
	if err != nil {
		return err
	}

	return b.conn().QueryRowContext(ctx, query, args...).Scan(b.returningDest...)
}

func (b Builder) Raw(ctx context.Context, dest any, query string, args ...any) error {
	return b.conn().SelectContext(ctx, dest, query, args)
}

func (b Builder) insertStructColumns(object any) Builder {
	if len(b.insertValues) == 0 {
		b.insertValues = make([][]any, 1)
	}

	v := reflect.Indirect(reflect.ValueOf(object))

	for i := 0; i < v.NumField(); i++ {
		dbTags := strings.Split(v.Type().Field(i).Tag.Get("db"), ",")
		if len(dbTags) == 0 || !slices.Contains(dbTags, modelTagColumn) {
			continue
		}
		columnName := dbTags[0]

		field := v.Field(i)
		value := field.Interface()

		if slices.Contains(dbTags, modelTagDefault) {
			var skip bool
			if valuer, ok := value.(driver.Valuer); ok {
				valuerValue, err := valuer.Value()
				if err == nil && (valuerValue == nil || valuerValue == reflect.Zero(reflect.TypeOf(valuerValue)).Interface()) {
					skip = true
				}
			} else {
				if value == reflect.Zero(field.Type()).Interface() {
					skip = true
				}
			}

			if skip {
				b.returningColumns = append(b.returningColumns, columnName)
				b.returningDest = append(b.returningDest, field.Addr().Interface())
				continue
			}
		}

		b.columns = append(b.columns, columnName)
		b.insertValues[0] = append(b.insertValues[0], value)
	}

	return b
}

func (b Builder) updateStructColumns(object any) Builder {
	v := reflect.Indirect(reflect.ValueOf(object))

	primaryKeys := make(map[string]any)

	for i := 0; i < v.NumField(); i++ {
		dbTags := strings.Split(v.Type().Field(i).Tag.Get("db"), ",")
		if len(dbTags) == 0 || !slices.Contains(dbTags, modelTagColumn) {
			continue
		}
		columnName := dbTags[0]

		field := v.Field(i)
		value := field.Interface()

		if slices.Contains(dbTags, modelTagPrimaryKey) {
			primaryKeys[columnName] = value
			continue
		}

		if len(b.columns) > 0 && !slices.Contains(b.columns, columnName) {
			continue
		}

		if columnName == ColumnCreatedAt {
			continue
		}
		if columnName == ColumnUpdatedAt {
			b.updateValues = append(b.updateValues, NewExpr(fmt.Sprintf("%s = DEFAULT", columnName)))
			b.returningColumns = append(b.returningColumns, columnName)
			b.returningDest = append(b.returningDest, field.Addr().Interface())
			continue
		}

		if valueString, ok := value.(string); ok && strings.EqualFold(valueString, "DEFAULT") {
			b.returningColumns = append(b.returningColumns, columnName)
			b.returningDest = append(b.returningDest, field.Addr().Interface())
		}

		b.updateValues = append(b.updateValues, NewExpr(fmt.Sprintf("%s = ?", columnName), value))
	}

	for columnName, value := range primaryKeys {
		b.whereExpr = append(b.whereExpr, NewExpr(fmt.Sprintf("%s = ?", columnName), value))
	}

	return b
}

func (b Builder) conn() Connection {
	if b.command == CommandSelect && b.readerConn != nil {
		return b.readerConn
	}
	return b.writerConn
}
