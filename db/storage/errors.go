package storage

import "errors"

var (
	ErrTableNotFound = errors.New("table not found")
	ErrDuplicate     = errors.New("duplicate key")
)
