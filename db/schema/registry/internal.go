package registry

import (
	"github.com/aliphe/filadb/db/object"
	"github.com/aliphe/filadb/db/schema"
)

const (
	internalTableTables  object.Table = "tables"
	internalTableColumns object.Table = "columns"
)

var internalTableTablesSchema = schema.Schema{
	Table: internalTableTables,
	Columns: []schema.Column{
		{
			Name: "id",
			Type: schema.ColumnTypeText,
		},
		{
			Name: "table",
			Type: schema.ColumnTypeText,
		},
		{
			Name: "version",
			Type: schema.ColumnTypeNumber,
		},
	},
}

var internalTableColumnsSchema = schema.Schema{
	Table: internalTableColumns,
	Columns: []schema.Column{
		{
			Name: "id",
			Type: schema.ColumnTypeText,
		},
		{
			Name: "table",
			Type: schema.ColumnTypeText,
		},
		{
			Name: "column",
			Type: schema.ColumnTypeText,
		},
		{
			Name: "type",
			Type: schema.ColumnTypeText,
		},
	},
}
