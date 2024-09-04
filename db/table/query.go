package table

import (
	"github.com/aliphe/filadb/db/index"
	"github.com/aliphe/filadb/db/object"
	"github.com/aliphe/filadb/db/storage"
)

// Querier is responsible for read-write operations on a given table.
type Querier[T object.Identifiable] struct {
	store     storage.ReaderWriter
	marshaler object.Marshaler
	table     object.Table
	idxs      []*index.Index
}

func NewQuerier[T object.Identifiable](store storage.ReaderWriter, marshaler object.Marshaler, idxs []*index.Index, table object.Table) *Querier[T] {
	return &Querier[T]{
		store:     store,
		marshaler: marshaler,
		table:     table,
		idxs:      idxs,
	}
}
