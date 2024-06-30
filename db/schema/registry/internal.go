package registry

import (
	"github.com/aliphe/filadb/db/object"
	"github.com/aliphe/filadb/db/schema"
)

const (
	internalTableTablesName  = "tables"
	internalTableColumnsName = "columns"
)

type internalTableTables struct {
	ID      object.ID
	Table   object.Table
	Version int
}

func (i internalTableTables) ObjectID() object.ID {
	return i.ID
}

func (i internalTableTables) ObjectTable() object.Table {
	return internalTableTablesName
}

var internalTableTablesSchema = &schema.Schema{
	Table: internalTableTablesName,
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

type internalTableColumns struct {
	ID     object.ID
	Table  object.Table
	Column string
	Type   string
}

func (i internalTableColumns) ObjectID() object.ID {
	return i.ID
}
func (i internalTableColumns) ObjectTable() object.Table {
	return internalTableColumnsName
}

var internalTableColumnsSchema = &schema.Schema{
	Table: internalTableColumnsName,
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
