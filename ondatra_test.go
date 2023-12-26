package ondatra

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBuilder_Insert(t *testing.T) {
	var tests = []struct {
		name        string
		builder     Builder
		expectQuery string
		expectArgs  []any
	}{
		{
			name:        "insert",
			builder:     NewEmptyBuilder().Insert().Into("test").Values(1, 2),
			expectQuery: "INSERT INTO test VALUES (?,?)",
			expectArgs:  []any{1, 2},
		}, {
			name: "insert all option",
			builder: NewEmptyBuilder().
				Prefix("WITH prefix AS ?", 0).
				Insert().
				Options("DELAYED", "IGNORE").
				Into("a").
				Columns("a", "b").
				Columns("c").
				Values(1, 2, 3).
				Values(4, 5, NewExpr("? + 1", 6)).
				Suffix("ON CONFLICT (b) DO NOTHING").
				Suffix("RETURNING ?", 7),
			expectQuery: "WITH prefix AS ? INSERT DELAYED IGNORE INTO a (a, b, c) VALUES (?,?,?),(?,?,? + 1) " +
				"ON CONFLICT (b) DO NOTHING RETURNING ?",
			expectArgs: []any{0, 1, 2, 3, 4, 5, 6, 7},
		}, {
			name: "insert sub query",
			builder: NewEmptyBuilder().
				Insert().
				Into("table2").
				Columns("field1").
				Values(
					NewEmptyBuilder().
						Select("field1").
						From("table1").
						Where("field1 = ?", 1),
				),
			expectQuery: "INSERT INTO table2 (field1) VALUES (SELECT field1 FROM table1 WHERE field1 = ?)",
			expectArgs:  []any{1},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			query, args, err := test.builder.ToSQL()
			assert.NoError(t, err)
			assert.Equal(t, test.expectQuery, query)
			assert.Equal(t, test.expectArgs, args)
		})
	}
}

func TestBuilder_Update(t *testing.T) {
	var tests = []struct {
		name        string
		builder     Builder
		expectQuery string
		expectArgs  []any
	}{
		{
			name: "update",
			builder: NewEmptyBuilder().
				Update().
				Table("a").
				Prefix("WITH prefix AS ?", 0).
				Set("b", NewExpr("? + 1", 1)).
				SetMap(map[string]interface{}{"c": 2}).
				Set("c1", NewExpr("CASE status WHEN 1 THEN 2 WHEN 2 THEN 1 END")).
				Set("c2", NewExpr("CASE WHEN a = 2 THEN ? WHEN a = 3 THEN ? END", "foo", "bar")).
				Where("d = ?", 3).
				OrderBy("e").
				Limit(4).
				Offset(5).
				Suffix("RETURNING ?", 6),
			expectQuery: "WITH prefix AS ? UPDATE a SET b = ? + 1, c = ?, " +
				"c1 = CASE status WHEN 1 THEN 2 WHEN 2 THEN 1 END, " +
				"c2 = CASE WHEN a = 2 THEN ? WHEN a = 3 THEN ? END " +
				"WHERE d = ? ORDER BY e LIMIT 4 OFFSET 5 RETURNING ?",
			expectArgs: []any{0, 1, 2, "foo", "bar", 3, 6},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			query, args, err := test.builder.ToSQL()
			assert.NoError(t, err)
			assert.Equal(t, test.expectQuery, query)
			assert.Equal(t, test.expectArgs, args)
		})
	}
}

func TestBuilder_Select(t *testing.T) {
	var tests = []struct {
		name        string
		builder     Builder
		expectQuery string
		expectArgs  []any
	}{
		{
			name:        "select",
			builder:     NewEmptyBuilder().Select("test").Where("x = ? AND y = ?"),
			expectQuery: "SELECT test WHERE x = ? AND y = ?",
			expectArgs:  nil,
		}, {
			name: "select all options",
			builder: NewEmptyBuilder().
				Select("a", "b", "c").
				Prefix("WITH prefix AS ?", 0).
				Distinct().
				From("e").
				SelectColumn("IF(d IN (?,?,?), 1, 0) as stat_column", 1, 2, 3).
				SelectColumn("a > ?", 100).
				SelectColumn("(b IN (?,?,?)) AS b_alias", 101, 102, 103).
				SelectColumn("(?) AS subq", NewEmptyBuilder().Select("aa", "bb").From("dd")).
				JoinRaw("CROSS JOIN j1").
				Join(JoinInner, "j2").
				Join(JoinLeft, "j3").
				Join(JoinRight, "j4").
				Where("f = ?", 4).
				Where("g = ?", 5).
				Where("h = ?", 6).
				Where("i IN (?,?,?)", 7, 8, 9).
				Where("(j = ? OR (k = ? AND true))", 10, 11).
				GroupBy("l").
				Having("m = n").
				OrderByArgs("? DESC", 1).
				OrderBy("o ASC", "p DESC").
				Limit(12).
				Offset(13).
				Suffix("FETCH FIRST ? ROWS ONLY", 14),
			expectQuery: "WITH prefix AS ? " +
				"SELECT DISTINCT a, b, c, IF(d IN (?,?,?), 1, 0) as stat_column, a > ?, " +
				"(b IN (?,?,?)) AS b_alias, (SELECT aa, bb FROM dd) AS subq " +
				"FROM e CROSS JOIN j1 INNER JOIN j2 LEFT JOIN j3 RIGHT JOIN j4 " +
				"WHERE f = ? AND g = ? AND h = ? AND i IN (?,?,?) AND (j = ? OR (k = ? AND true)) " +
				"GROUP BY l HAVING m = n ORDER BY ? DESC, o ASC, p DESC LIMIT 12 OFFSET 13 " +
				"FETCH FIRST ? ROWS ONLY",
			expectArgs: []any{0, 1, 2, 3, 100, 101, 102, 103, 4, 5, 6, 7, 8, 9, 10, 11, 1, 14},
		}, {
			name: "select from select",
			builder: NewEmptyBuilder().
				Select("a", "b").
				FromSelect(
					NewEmptyBuilder().
						Select("c").
						From("d").
						Where("i = ?", 0),
					"subq",
				),
			expectQuery: "SELECT a, b FROM (SELECT c FROM d WHERE i = ?) AS subq",
			expectArgs:  []any{0},
		}, {
			name: "select from select 2",
			builder: NewEmptyBuilder().Select("c").
				FromSelect(
					NewEmptyBuilder().
						Select("c").
						From("t").
						Where("c > ?", 1).
						PlaceholderFormat(Dollar),
					"subq",
				).
				Where("c < ?", 2).
				PlaceholderFormat(Dollar),
			expectQuery: "SELECT c FROM (SELECT c FROM t WHERE c > $1) AS subq WHERE c < $2",
			expectArgs:  []any{1, 2},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			query, args, err := test.builder.ToSQL()
			assert.NoError(t, err)
			assert.Equal(t, test.expectQuery, query)
			assert.Equal(t, test.expectArgs, args)
		})
	}
}

func TestBuilder_Delete(t *testing.T) {
	var tests = []struct {
		name        string
		builder     Builder
		expectQuery string
		expectArgs  []any
	}{
		{
			name: "delete",
			builder: NewEmptyBuilder().
				Delete().
				Prefix("WITH prefix AS ?", 0).
				From("a").
				Where("b = ?", 1).
				OrderBy("c").
				Limit(2).
				Offset(3).
				Suffix("RETURNING ?", 4),
			expectQuery: "WITH prefix AS ? DELETE FROM a WHERE b = ? ORDER BY c LIMIT 2 OFFSET 3 RETURNING ?",
			expectArgs:  []any{0, 1, 4},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			query, args, err := test.builder.ToSQL()
			assert.NoError(t, err)
			assert.Equal(t, test.expectQuery, query)
			assert.Equal(t, test.expectArgs, args)
		})
	}
}
