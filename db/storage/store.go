package storage

import (
	"context"
)

type ReaderWriter interface {
	Reader
	Writer
}

type Writer interface {
	Add(ctx context.Context, node, key string, val []byte) error
}

type Reader interface {
	Get(ctx context.Context, table, key string) ([]byte, bool, error)
	Scan(ctx context.Context, table string) ([][]byte, error)
}
