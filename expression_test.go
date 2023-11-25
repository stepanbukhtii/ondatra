package ondatra

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExpr_ToSQL(t *testing.T) {
	var tests = []struct {
		name        string
		expr        Expr
		expectQuery string
		expectArgs  []any
	}{
		{
			name:        "simple",
			expr:        NewExpr("a > ? AND b > ? AND c > ?", 1, 2, 3),
			expectQuery: "a > ? AND b > ? AND c > ?",
			expectArgs:  []any{1, 2, 3},
		}, {
			name: "with sub query",
			expr: NewExpr("t > ? AND (?) AND c > ? AND (?)",
				1,
				NewExpr("a > ?", 2),
				3,
				NewExpr("? AND j > ?", NewExpr("h > ?", 4), 5),
			),
			expectQuery: "t > ? AND (a > ?) AND c > ? AND (h > ? AND j > ?)",
			expectArgs:  []any{1, 2, 3, 4, 5},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			query, args, err := test.expr.ToSQL()
			assert.NoError(t, err)
			assert.Equal(t, test.expectQuery, query)
			assert.Equal(t, test.expectArgs, args)
		})
	}
}
