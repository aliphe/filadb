package system

import (
	"context"

	"github.com/aliphe/filadb/db/object"
	"github.com/aliphe/filadb/db/schema"
)

type DatabaseShape struct {
	schemas     map[object.Table]*schema.Schema
	colMappings map[string][]object.Table
	allCols     map[string]bool
}

func NewDatabaseShape(schemas []*schema.Schema) *DatabaseShape {
	byTable := make(map[object.Table]*schema.Schema)
	colMappings := make(map[string][]object.Table)
	allCols := make(map[string]bool)
	for _, sch := range schemas {
		byTable[sch.Table] = sch
		for _, c := range sch.Columns {
			colMappings[c.Name] = append(colMappings[c.Name], sch.Table)
			allCols[object.Key(sch.Table, c.Name)] = true
		}
	}
	return &DatabaseShape{
		byTable,
		colMappings,
		allCols,
	}
}

func (d *DatabaseShape) ColMappings() map[string][]object.Table {
	return d.colMappings
}

func (d *DatabaseShape) AllCols() map[string]bool {
	return d.allCols
}

func (sr *SchemaRegistry) Shape(ctx context.Context) (*DatabaseShape, error) {
	var tables []internalTableTables
	err := sr.tables.Scan(ctx, &tables)
	if err != nil {
		return nil, err
	}

	schemas := make([]*schema.Schema, 0, len(tables))
	for _, t := range tables {
		sch, err := sr.loadSchema(context.Background(), object.Table(t.Table))
		if err != nil {
			return nil, err
		}
		schemas = append(schemas, sch)
	}

	return NewDatabaseShape(schemas), nil
}
