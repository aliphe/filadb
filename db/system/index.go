package system

import (
	"context"
	"errors"
	"strings"

	"github.com/aliphe/filadb/db/index"
	"github.com/aliphe/filadb/db/object"
	"github.com/aliphe/filadb/db/schema"
	"github.com/aliphe/filadb/db/storage"
	"github.com/aliphe/filadb/db/table"
)

const (
	columnSeparator = ","
)

type IndexRegistry struct {
	store   storage.ReaderWriter
	tables  *table.Querier[internalTableTables]
	indexes *table.Querier[internalTableIndexes]
}

func NewIndexRegistry(store storage.ReaderWriter) *IndexRegistry {
	return &IndexRegistry{
		store:   store,
		tables:  table.NewQuerier[internalTableTables](store, internalTableTablesSchema.Marshaler(), object.Table(internalTableTablesName)),
		indexes: table.NewQuerier[internalTableIndexes](store, internalTableIndexesSchema.Marshaler(), object.Table(internalTableIndexesName)),
	}
}

func (ir *IndexRegistry) Scan(ctx context.Context, t object.Table) ([]*index.Index, error) {
	var raw []internalTableIndexes
	err := ir.indexes.Scan(ctx, &raw)
	if err != nil {
		if errors.Is(err, storage.ErrTableNotFound) {
			return nil, nil
		}
		return nil, err
	}

	idxs := make([]*index.Index, 0, len(raw))
	for _, idx := range raw {
		idxs = append(idxs, idx.Index())
	}

	return idxs, nil
}

func (ir *IndexRegistry) Create(ctx context.Context, idx *index.Index) error {
	err := ir.indexes.Insert(ctx, fromIndex(idx))
	if err != nil {
		return nil
	}

	err = ir.tables.Insert(ctx, internalTableTables{
		ID:      object.ID(idx.Name),
		Table:   idx.Table,
		Version: 1,
	})
	if err != nil {
		return err
	}

	return nil
}

func (ir *IndexRegistry) Index(ctx context.Context, idx *index.Index, rows ...object.Row) error {
	for _, row := range rows {
		key := idx.Key(row)
		if err := ir.store.Add(ctx, idx.Name, string(key), []byte(row.ObjectID())); err != nil {
			return err
		}
	}

	return nil
}

type internalTableIndexes struct {
	Table   object.Table
	Name    string
	Columns string
}

func (i internalTableIndexes) Index() *index.Index {
	return &index.Index{
		Table:   i.Table,
		Name:    i.Name,
		Columns: strings.Split(i.Columns, columnSeparator),
	}
}

func fromIndex(idx *index.Index) internalTableIndexes {
	return internalTableIndexes{
		Table:   idx.Table,
		Name:    idx.Name,
		Columns: strings.Join(idx.Columns, columnSeparator),
	}
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
