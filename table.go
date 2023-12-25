package ondatra

import "fmt"

type Table struct {
	name    string
	columns []string
}

func NewTable(name string, columns []string) Table {
	return Table{
		name:    name,
		columns: columns,
	}
}

func (t Table) Name() string {
	return t.name
}

func (t Table) Columns() []string {
	var columns []string
	for _, column := range t.columns {
		columns = append(columns, fmt.Sprintf("%s.%s", t.name, column))
	}
	return columns
}

func (t Table) ColumnsAlias(prefix, alias string) []string {
	var columns []string
	for _, column := range t.columns {
		columns = append(columns, fmt.Sprintf("%s.%s AS \"%s.%s\"", prefix, column, alias, column))
	}
	return columns
}
