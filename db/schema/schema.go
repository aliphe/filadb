package schema

import (
	"github.com/aliphe/filadb/db/object"
)

type Schema struct {
	version int32
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

const (
	internalTableTables  object.Table = "tables"
	internalTableColumns object.Table = "columns"
)

var internalTableTablesSchema = Schema{
	Table: internalTableTables,
	Columns: []Column{
		{
			Name: "id",
			Type: ColumnTypeText,
		},
		{
			Name: "table",
			Type: ColumnTypeText,
		},
		{
			Name: "version",
			Type: ColumnTypeNumber,
		},
	},
}

var internalTableColumnsSchema = Schema{
	Table: internalTableColumns,
	Columns: []Column{
		{
			Name: "id",
			Type: ColumnTypeText,
		},
		{
			Name: "table",
			Type: ColumnTypeText,
		},
		{
			Name: "column",
			Type: ColumnTypeText,
		},
		{
			Name: "type",
			Type: ColumnTypeText,
		},
	},
}
