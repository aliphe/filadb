package schema

import (
	"github.com/aliphe/filadb/db/object"
)

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
