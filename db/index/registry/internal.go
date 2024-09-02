package registry

import (
	"github.com/aliphe/filadb/db/object"
	"github.com/aliphe/filadb/db/schema"
)

const (
	internalTableIndexesName        = "indexes"
	internalTableIndexedColumnsName = "indexed_columns"
)

// internalTableIndexes is used to store all the indexes definitions.
type internalTableIndexes struct {
	ID    object.ID
	Table object.Table
}

func (i internalTableIndexes) ObjectID() object.ID {
	return i.ID
}

func (i internalTableIndexes) ObjectTable() object.Table {
	return internalTableIndexesName
}

var internalTableIndexesSchema = &schema.Schema{
	Table: internalTableIndexesName,
	Columns: []schema.Column{
		{
			Name: "id",
			Type: schema.ColumnTypeText,
		},
		{
			Name: "table",
			Type: schema.ColumnTypeText,
		},
	},
}

// internalTableIndexedColumns is used to store all the indexed columns.
type internalTableIndexedColumns struct {
	ID     object.ID
	Table  object.ID
	Column string
}

func (i internalTableIndexedColumns) ObjectID() object.ID {
	return i.ID
}

func (i internalTableIndexedColumns) ObjectTable() object.Table {
	return internalTableIndexedColumnsName
}

var internalTableIndexedColumnsSchema = &schema.Schema{
	Table: internalTableIndexedColumnsName,
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
	},
}
