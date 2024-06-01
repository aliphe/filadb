package errors

import "errors"

var (
	ErrDatabaseNotSeeded = errors.New("database not seeded")
	ErrTableNotFound     = errors.New("table not found")
)

type RequiredPropertyError struct {
	Property string
}

func (r RequiredPropertyError) Error() string {
	return "missing required property: \"" + r.Property + "\""
}
