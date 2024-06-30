package schema

import (
	"github.com/aliphe/filadb/db/object"
)

type MarshalerFactory func(*Schema) object.Marshaler

type Schema struct {
	Table   object.Table
	Columns []Column
}

type Column struct {
	Name string
	Type ColumnType
}

type ColumnType string

const (
	ColumnTypeText   ColumnType = "text"
	ColumnTypeNumber ColumnType = "number"
)
