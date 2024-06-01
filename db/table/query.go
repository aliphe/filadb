package table

import (
	"github.com/aliphe/filadb/db/object"
	"github.com/aliphe/filadb/db/storage"
)

// Querier is responsible for read-write operations on a given table.
type Querier struct {
	store     storage.ReaderWriter
	marshaler marshaler
	table     object.Table
}

func NewQuerier(store storage.ReaderWriter, marshaler marshaler, table object.Table) *Querier {
	return &Querier{
		store:     store,
		marshaler: marshaler,
		table:     table,
	}
}
