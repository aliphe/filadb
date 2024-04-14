package data

import "github.com/aliphe/filadb/db/storage"

type DB struct {
	s storage.ReaderWriter
}

func New(s storage.ReaderWriter) *DB {
	return &DB{
		s: s,
	}
}
