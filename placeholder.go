package ondatra

import (
	"strconv"
	"strings"
)

type PlaceholderFormat interface {
	ReplacePlaceholders(sql string) string
}

var (
	// Dollar is a PlaceholderFormat instance that replaces placeholders with
	// dollar-prefixed positional placeholders (e.g. $1, $2, $3).
	Dollar = stringPlaceholderFormat("$")

	// Colon is a PlaceholderFormat instance that replaces placeholders with
	// colon-prefixed positional placeholders (e.g. :1, :2, :3).
	Colon = stringPlaceholderFormat(":")

	// AtP is a PlaceholderFormat instance that replaces placeholders with
	// "@p"-prefixed positional placeholders (e.g. @p1, @p2, @p3).
	AtP = stringPlaceholderFormat("@p")
)

type stringPlaceholderFormat string

func (s stringPlaceholderFormat) ReplacePlaceholders(sql string) string {
	var buffer strings.Builder
	var i int
	for {
		p := strings.Index(sql, "?")
		if p == -1 {
			break
		}

		if len(sql[p:]) > 1 && sql[p:p+2] == "??" { // escape ?? => ?
			buffer.WriteString(sql[:p])
			buffer.WriteString("?")
			if len(sql[p:]) == 1 {
				break
			}
			sql = sql[p+2:]
		} else {
			i++

			buffer.WriteString(sql[:p])
			buffer.WriteString(string(s))
			buffer.WriteString(strconv.Itoa(i))

			sql = sql[p+1:]
		}
	}

	buffer.WriteString(sql)

	return buffer.String()
}
