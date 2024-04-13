package db

import "context"

type DB struct {
	s storage
}

type storage interface {
	Add(ctx context.Context, key string, val []byte) error
	Get(ctx context.Context, key string) ([]byte, bool, error)
}

func New(s storage) *DB {
	return &DB{
		s: s,
	}
}
