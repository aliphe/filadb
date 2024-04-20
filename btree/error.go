package btree

import "errors"

var (
	ErrTreeCorrupted = errors.New("tree corrupted")
	ErrNodeNotFound  = errors.New("node not found")
)
