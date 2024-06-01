package schema

import (
	"context"
	"errors"
	"fmt"

	"github.com/aliphe/filadb/db/object"
	"github.com/aliphe/filadb/db/storage"
	"github.com/aliphe/filadb/db/table"
)

var (
	ErrTableNotFound = errors.New("schema not found")
)

type Admin struct {
	tables     *table.Querier
	columns    *table.Querier
	marshalers map[object.Table]Marshaler
	factory    MarshalerFactory
}

func NewAdmin(store storage.ReaderWriter, factory MarshalerFactory) (*Admin, error) {
	tables := table.NewQuerier(store, factory(&internalTableTablesSchema), internalTableTables)
	columns := table.NewQuerier(store, factory(&internalTableColumnsSchema), internalTableColumns)

	a := &Admin{
		tables:     tables,
		columns:    columns,
		marshalers: make(map[object.Table]Marshaler),
		factory:    factory,
	}

	err := a.load()
	if err != nil {
		return nil, err
	}
	return a, nil
}

func (a *Admin) load() error {
	s, err := a.tables.Scan(context.Background())
	if err != nil && !errors.Is(err, storage.ErrTableNotFound) {
		return err
	}

	for _, t := range s {
		table := t["table"].(string)
		mar, err := a.fromStorage(context.Background(), object.Table(table))
		if err != nil {
			return err
		}
		a.marshalers[object.Table(table)] = mar
	}

	a.marshalers[internalTableTables] = a.factory(&internalTableTablesSchema)
	a.marshalers[internalTableColumns] = a.factory(&internalTableColumnsSchema)
	return nil
}

func (a *Admin) Marshalers() map[object.Table]Marshaler {
	return a.marshalers
}

func (a *Admin) Create(ctx context.Context, schema *Schema) error {
	err := a.createTable(ctx, schema.Table)
	if err != nil {
		return err
	}

	err = a.createColumns(ctx, schema.Table, schema.Columns)
	if err != nil {
		return err
	}

	a.marshalers[schema.Table] = a.factory(schema)

	return nil
}

func (a *Admin) createTable(ctx context.Context, table object.Table) error {
	err := a.tables.Insert(ctx, object.Row{
		"id":      string(table),
		"table":   string(table),
		"version": 1,
	})
	if err != nil {
		return fmt.Errorf("save schema: %w", err)
	}

	return nil
}

func (a *Admin) createColumns(ctx context.Context, table object.Table, cols []Column) error {
	for _, col := range cols {
		row := object.Row{
			"id":     string(table) + col.Name,
			"table":  string(table),
			"column": col.Name,
			"type":   string(col.Type),
		}
		err := a.columns.Insert(ctx, row)
		if err != nil {
			return fmt.Errorf("save column %s: %w", col.Name, err)
		}
	}

	return nil
}
