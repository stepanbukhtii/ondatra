package ondatra

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestJoinBuilder(t *testing.T) {
	b := NewJoinBuilder("companies")

	var tests = []struct {
		table        string
		alias        string
		field        string
		relatedField string
		expectQuery  string
	}{
		{
			table:        "users",
			alias:        "owner",
			field:        "id",
			relatedField: "owner_id",
			expectQuery:  "LEFT JOIN users as \"owner\" ON \"owner\".id = \"companies\".owner_id",
		}, {
			table:        "companies",
			alias:        "manager_companies",
			field:        "manager_id",
			relatedField: "id",
			expectQuery:  "LEFT JOIN companies as \"owner.manager_companies\" ON \"owner.manager_companies\".manager_id = \"owner\".id",
		}, {
			table:        "users",
			alias:        "owner",
			field:        "id",
			relatedField: "owner_id",
			expectQuery:  "LEFT JOIN users as \"owner.manager_companies.owner\" ON \"owner.manager_companies.owner\".id = \"owner.manager_companies\".owner_id",
		},
	}

	for _, test := range tests {
		b = b.NewJoin(JoinLeft, test.table, test.alias, test.field, test.relatedField)
		query, _, err := b.ToSQL()
		assert.NoError(t, err)
		assert.Equal(t, test.expectQuery, query)
	}
}
