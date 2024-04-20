package table

import "github.com/aliphe/filadb/db/storage"

type Store struct {
	s storage.ReaderWriter
}

func NewStore(s storage.ReaderWriter) *Store {
	return &Store{
		s: s,
	}
}
