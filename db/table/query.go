package table

import (
	"github.com/aliphe/filadb/db/object"
	"github.com/aliphe/filadb/db/storage"
)

type identifiable interface {
	ObjectID() object.ID
	ObjectTable() object.Table
}

// Querier is responsible for read-write operations on a given table.
type Querier[T identifiable] struct {
	store     storage.ReaderWriter
	marshaler object.Marshaler
	table     object.Table
}

func NewQuerier[T identifiable](store storage.ReaderWriter, marshaler object.Marshaler, table object.Table) *Querier[T] {
	return &Querier[T]{
		store:     store,
		marshaler: marshaler,
		table:     table,
	}
}
