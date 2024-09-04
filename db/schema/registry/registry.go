package registry

import (
	"context"
	"errors"
	"fmt"

	"github.com/aliphe/filadb/db/object"
	"github.com/aliphe/filadb/db/schema"
	"github.com/aliphe/filadb/db/storage"
	"github.com/aliphe/filadb/db/table"
)

var (
	ErrTableNotFound = errors.New("table not found")
)

type Registry struct {
	store       storage.ReaderWriter
	tables      *table.Querier[internalTableTables]
	columns     *table.Querier[internalTableColumns]
	marshallers map[object.Table]object.Marshaler
	factory     schema.MarshalerFactory
}

func New(store storage.ReaderWriter, factory schema.MarshalerFactory) (*Registry, error) {
	tables := table.NewQuerier[internalTableTables](store, factory(internalTableTablesSchema), nil, internalTableTablesName)
	columns := table.NewQuerier[internalTableColumns](store, factory(internalTableColumnsSchema), nil, internalTableColumnsName)

	a := &Registry{
		store:       store,
		tables:      tables,
		columns:     columns,
		marshallers: make(map[object.Table]object.Marshaler),
		factory:     factory,
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
		a.marshallers[t.ObjectTable()] = mar
	}

	a.marshallers[internalTableTablesName] = a.factory(internalTableTablesSchema)
	a.marshallers[internalTableColumnsName] = a.factory(internalTableColumnsSchema)
	return nil
}

func (r *Registry) Marshaller(ctx context.Context, t object.Table) (object.Marshaler, error) {
	m, ok := r.marshallers[t]
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

	a.marshallers[schema.Table] = a.factory(schema)

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
