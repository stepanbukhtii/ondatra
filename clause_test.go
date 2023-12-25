package ondatra

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestClause(t *testing.T) {
	var tests = []struct {
		name        string
		clauses     []Clause
		expectQuery string
		expectArgs  []any
	}{
		{
			name: "select",
			clauses: []Clause{
				SelectColumns("id"),
				Where("x = ? AND y = ?"),
			},
			expectQuery: "SELECT id FROM test WHERE x = ? AND y = ?",
			expectArgs:  nil,
		}, {
			name: "select all options",
			clauses: []Clause{
				SelectColumns("a", "b", "c"),
				Prefix("WITH prefix AS ?", 0),
				Distinct(),
				SelectColumn("IF(d IN (?,?,?), 1, 0) as stat_column", 1, 2, 3),
				SelectColumn("a > ?", 100),
				SelectColumn("(b IN (?,?,?)) AS b_alias", 101, 102, 103),
				SelectColumn("(?) AS subq", NewEmptyBuilder().Select("aa", "bb").From("dd")),
				JoinRaw("CROSS JOIN j1"),
				Join(JoinInner, "j2"),
				Join(JoinLeft, "j3"),
				Join(JoinRight, "j4"),
				Where("f = ?", 4),
				Where("g = ?", 5),
				Where("h = ?", 6),
				Where("i IN (?,?,?)", 7, 8, 9),
				Where("(j = ? OR (k = ? AND true))", 10, 11),
				GroupBy("l"),
				Having("m = n"),
				OrderByArgs("? DESC", 1),
				OrderBy("o ASC", "p DESC"),
				Limit(12),
				Offset(13),
				Suffix("FETCH FIRST ? ROWS ONLY", 14),
			},
			expectQuery: "WITH prefix AS ? " +
				"SELECT DISTINCT a, b, c, IF(d IN (?,?,?), 1, 0) as stat_column, a > ?, " +
				"(b IN (?,?,?)) AS b_alias, (SELECT aa, bb FROM dd) AS subq " +
				"FROM test CROSS JOIN j1 INNER JOIN j2 LEFT JOIN j3 RIGHT JOIN j4 " +
				"WHERE f = ? AND g = ? AND h = ? AND i IN (?,?,?) AND (j = ? OR (k = ? AND true)) " +
				"GROUP BY l HAVING m = n ORDER BY ? DESC, o ASC, p DESC LIMIT 12 OFFSET 13 " +
				"FETCH FIRST ? ROWS ONLY",
			expectArgs: []any{0, 1, 2, 3, 100, 101, 102, 103, 4, 5, 6, 7, 8, 9, 10, 11, 1, 14},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			query, args, err := NewEmptyBuilder().Select().From("test").Clauses(test.clauses...).ToSQL()
			assert.NoError(t, err)
			assert.Equal(t, test.expectQuery, query)
			assert.Equal(t, test.expectArgs, args)
		})
	}
}
