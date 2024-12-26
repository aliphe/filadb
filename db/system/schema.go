package system

import (
	"context"
	"fmt"

	"github.com/aliphe/filadb/db/object"
	"github.com/aliphe/filadb/db/schema"
	"github.com/aliphe/filadb/db/storage"
	"github.com/aliphe/filadb/db/table"
)

type SchemaRegistry struct {
	tables  *table.Querier[internalTableTables]
	columns *table.Querier[internalTableColumns]
}

func NewSchemaRegistry(store storage.ReaderWriter) *SchemaRegistry {
	return &SchemaRegistry{
		tables:  table.NewQuerier[internalTableTables](store, internalTableTablesSchema.Marshaler(), object.Table(internalTableTablesName)),
		columns: table.NewQuerier[internalTableColumns](store, internalTableColumnsSchema.Marshaler(), object.Table(internalTableColumnsName)),
	}
}

func (sr *SchemaRegistry) Create(ctx context.Context, sch *schema.Schema) error {
	err := sr.createTable(ctx, sch.Table)
	if err != nil {
		return err
	}

	err = sr.createColumns(ctx, sch.Table, sch.Columns)
	if err != nil {
		return err
	}

	return nil
}

func (sr *SchemaRegistry) Get(ctx context.Context, table object.Table) (*schema.Schema, error) {
	var t internalTableTables
	err := sr.tables.Get(ctx, string(table), &t)
	if err != nil {
		return nil, err
	}

	sch, err := sr.loadSchema(context.Background(), object.Table(t.Table))
	if err != nil {
		return nil, err
	}

	return sch, nil
}

func (sr *SchemaRegistry) createTable(ctx context.Context, table object.Table) error {
	err := sr.tables.Insert(ctx, internalTableTables{
		ID:      object.ID(table),
		Table:   table,
		Version: 1,
	})
	if err != nil {
		return fmt.Errorf("save schema: %w", err)
	}

	return nil
}

func (sr *SchemaRegistry) createColumns(ctx context.Context, table object.Table, cols []schema.Column) error {
	for _, col := range cols {
		row := internalTableColumns{
			ID:     object.ID(table) + object.ID(col.Name),
			Table:  table,
			Column: col.Name,
			Type:   string(col.Type),
		}
		err := sr.columns.Insert(ctx, row)
		if err != nil {
			return fmt.Errorf("save column %s: %w", col.Name, err)
		}
	}

	return nil
}

func (sr *SchemaRegistry) loadSchema(ctx context.Context, table object.Table) (*schema.Schema, error) {
	out := schema.Schema{
		Table: table,
	}
	cols := make([]internalTableColumns, 0)
	err := sr.columns.Scan(ctx, &cols)
	if err != nil {
		return nil, err
	}
	for _, c := range cols {
		if c.Table == table {
			out.Columns = append(out.Columns, schema.Column{
				Name: c.Column,
				Type: schema.ColumnType(c.Type),
			})
		}
	}

	return &out, nil
}

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
