package btree

import "errors"

type Error string

func (e Error) Error() string {
	return string(e)
}

var (
	ErrTreeCorrupted = errors.New("tree corrupted")
)
