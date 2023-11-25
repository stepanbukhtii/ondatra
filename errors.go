package ondatra

import "errors"

var (
	SqlDBNotSet             = errors.New("cannot run; no sql db set")
	ErrAlreadyInTransaction = errors.New("already in transaction")
	NotSetColumns           = errors.New("columns must have at least one set of values")
	NotSetValues            = errors.New("values must have at least one set of values")
)
