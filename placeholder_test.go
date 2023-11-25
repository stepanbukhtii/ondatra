package ondatra

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestStringPlaceholderFormat_ReplacePlaceholders(t *testing.T) {
	var tests = []struct {
		name        string
		placeholder PlaceholderFormat
		expect      string
	}{
		{
			name:        "dollar placeholder",
			placeholder: Dollar,
			expect:      "SELECT test WHERE x = $1 AND y = $2",
		}, {
			name:        "colon placeholder",
			placeholder: Colon,
			expect:      "SELECT test WHERE x = :1 AND y = :2",
		}, {
			name:        "atP placeholder",
			placeholder: AtP,
			expect:      "SELECT test WHERE x = @p1 AND y = @p2",
		},
	}

	sql := "SELECT test WHERE x = ? AND y = ?"
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expect, test.placeholder.ReplacePlaceholders(sql))
		})
	}
}
