package storage

import "context"

type ReaderWriter interface {
	Reader
	Writer
}

type Writer interface {
	Add(ctx context.Context, table string, key string, val []byte) error
}

type Reader interface {
	Get(ctx context.Context, table string, key string) ([]byte, bool, error)
}
