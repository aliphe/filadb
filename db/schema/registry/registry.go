package registry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/aliphe/filadb/db/object"
	"github.com/aliphe/filadb/db/schema"
	"github.com/aliphe/filadb/db/storage"
	"github.com/aliphe/filadb/db/table"
)

var (
	ErrTableNotFound = errors.New("table not found")
)

type Registry struct {
	store   storage.ReaderWriter
	tables  *table.Querier[internalTableTables]
	columns *table.Querier[internalTableColumns]
	schemas map[object.Table]*schema.Schema
}

func New(store storage.ReaderWriter) (*Registry, error) {
	tables := table.NewQuerier[internalTableTables](store, internalTableTablesSchema.Marshaler(), internalTableTablesName)
	columns := table.NewQuerier[internalTableColumns](store, internalTableColumnsSchema.Marshaler(), internalTableColumnsName)

	a := &Registry{
		store:   store,
		tables:  tables,
		columns: columns,
		schemas: make(map[object.Table]*schema.Schema),
	}

	err := a.load()
	if err != nil {
		return nil, fmt.Errorf("init schema registry: %w", err)
	}
	return a, nil
}

func (a *Registry) load() error {
	s := make([]internalTableTables, 0)
	err := a.tables.Scan(context.Background(), &s)
	if err != nil && !errors.Is(err, storage.ErrTableNotFound) {
		return err
	}

	for _, t := range s {
		mar, err := a.fromStorage(context.Background(), object.Table(t.Table))
		if err != nil {
			return err
		}
		slog.Debug("loaded from storage", "table", t.Table)
		a.schemas[t.Table] = mar
	}

	a.schemas["tables"] = internalTableTablesSchema
	a.schemas["columns"] = internalTableColumnsSchema
	return nil
}

func (r *Registry) Get(ctx context.Context, id object.Table) (*schema.Schema, error) {
	m, ok := r.schemas[object.Table(id)]
	if !ok {
		return nil, ErrTableNotFound
	}

	return m, nil
}

func (a *Registry) Create(ctx context.Context, schema *schema.Schema) error {
	err := a.createTable(ctx, schema.Table)
	if err != nil {
		return err
	}

	err = a.createColumns(ctx, schema.Table, schema.Columns)
	if err != nil {
		return err
	}

	a.schemas[schema.Table] = schema

	return nil
}

func (a *Registry) createTable(ctx context.Context, table object.Table) error {
	err := a.tables.Insert(ctx, internalTableTables{
		ID:      object.ID(table),
		Table:   table,
		Version: 1,
	})
	if err != nil {
		return fmt.Errorf("save schema: %w", err)
	}

	return nil
}

func (a *Registry) createColumns(ctx context.Context, table object.Table, cols []schema.Column) error {
	for _, col := range cols {
		row := internalTableColumns{
			ID:     object.ID(table) + object.ID(col.Name),
			Table:  table,
			Column: col.Name,
			Type:   string(col.Type),
		}
		err := a.columns.Insert(ctx, row)
		if err != nil {
			return fmt.Errorf("save column %s: %w", col.Name, err)
		}
	}

	return nil
}
