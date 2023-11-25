package ondatra

import (
	"io"
	"strings"
)

type Expr interface {
	ToSQL() (string, []any, error)
}

type expr struct {
	rawSQL string
	args   []any
}

func NewExpr(rawSQL string, args ...any) Expr {
	return expr{rawSQL: rawSQL, args: args}
}

func (e expr) ToSQL() (string, []any, error) {
	if len(e.args) == 0 {
		return e.rawSQL, nil, nil
	}

	var args []any
	for i, arg := range e.args {
		switch a := arg.(type) {
		case Builder:
			a = a.PlaceholderFormat(nil)
			newSQL, newArgs, err := a.ToSQL()
			if err != nil {
				return "", nil, err
			}

			e.rawSQL = replaceNth(e.rawSQL, "?", newSQL, i+1)
			args = append(args, newArgs...)
		case Expr:
			newSQL, newArgs, err := a.ToSQL()
			if err != nil {
				return "", nil, err
			}

			e.rawSQL = replaceNth(e.rawSQL, "?", newSQL, i+1)
			args = append(args, newArgs...)
		default:
			args = append(args, arg)
		}
	}

	return e.rawSQL, args, nil
}

// Replace the nth occurrence of old in s by new.
func replaceNth(s, old, new string, n int) string {
	i := 0
	for m := 1; m <= n; m++ {
		x := strings.Index(s[i:], old)
		if x < 0 {
			break
		}
		i += x
		if m == n {
			return s[:i] + new + s[i+len(old):]
		}
		i += len(old)
	}
	return s
}

func writeExpr(expr Expr, w io.Writer, args []any) ([]any, error) {
	sql, sqlArgs, err := expr.ToSQL()
	if err != nil {
		return nil, err
	}
	if len(sql) == 0 {
		return nil, nil
	}

	if _, err = io.WriteString(w, sql); err != nil {
		return nil, err
	}

	args = append(args, sqlArgs...)

	return args, nil
}

func writeExprs(expr []Expr, w io.Writer, sep string, args []any) ([]any, error) {
	var err error
	for i, e := range expr {
		if i > 0 {
			if _, err := io.WriteString(w, sep); err != nil {
				return nil, err
			}
		}

		args, err = writeExpr(e, w, args)
		if err != nil {
			return nil, err
		}
	}
	return args, nil
}
