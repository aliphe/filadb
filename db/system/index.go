package system

import (
	"context"

	"github.com/aliphe/filadb/db/index"
	"github.com/aliphe/filadb/db/object"
	"github.com/aliphe/filadb/db/schema"
	"github.com/aliphe/filadb/db/storage"
	"github.com/aliphe/filadb/db/table"
)

type IndexRegistry struct {
	tables  *table.Querier[internalTableTables]
	indexes *table.Querier[internalTableIndexes]
}

func NewIndexRegistry(store storage.ReaderWriter) *IndexRegistry {
	return &IndexRegistry{
		tables:  table.NewQuerier[internalTableTables](store, internalTableTablesSchema.Marshaler(), object.Table(internalTableTablesName)),
		indexes: table.NewQuerier[internalTableIndexes](store, internalTableIndexesSchema.Marshaler(), object.Table(internalTableIndexesName)),
	}
}

func (ir *IndexRegistry) Scan(ctx context.Context, t object.Table) ([]*index.Index, error) {
	return nil, nil
}

func (ir *IndexRegistry) Create(ctx context.Context, idx *index.Index) error {
	return nil
}

type internalTableIndexes struct {
	Table   object.Table
	Name    string
	Columns string
}

func (i internalTableIndexes) ObjectID() object.ID {
	return object.ID(i.Name)
}

func (i internalTableIndexes) ObjectTable() object.Table {
	return internalTableIndexesName
}

var internalTableIndexesSchema = &schema.Schema{
	Table: internalTableIndexesName,
	Columns: []schema.Column{
		{
			Name: "name",
			Type: schema.ColumnTypeText,
		},
		{
			Name: "table",
			Type: schema.ColumnTypeText,
		},
		{
			Name: "columns",
			Type: schema.ColumnTypeText,
		},
	},
}
