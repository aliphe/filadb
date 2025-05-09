package system

import (
	"context"
	"errors"
	"slices"

	"github.com/aliphe/filadb/db/object"
	"github.com/aliphe/filadb/db/schema"
	"github.com/aliphe/filadb/db/storage"
)

type DatabaseShape struct {
	Schemas     map[object.Table]*schema.Schema
	ColMappings map[string][]object.Table
	AllCols     map[object.Table]map[string]bool
}

func NewDatabaseShape(schemas []*schema.Schema) *DatabaseShape {
	byTable := make(map[object.Table]*schema.Schema)
	colMappings := make(map[string][]object.Table)
	allCols := make(map[object.Table]map[string]bool)
	for _, sch := range schemas {
		byTable[sch.Table] = sch
		allCols[sch.Table] = make(map[string]bool)
		for _, c := range sch.Columns {
			colMappings[c.Name] = append(colMappings[c.Name], sch.Table)
			allCols[sch.Table][c.Name] = true
		}
	}
	return &DatabaseShape{
		byTable,
		colMappings,
		allCols,
	}
}

func (sr *SchemaRegistry) Shape(ctx context.Context, onlyTables []object.Table) (*DatabaseShape, error) {
	var tables []internalTableTables
	err := sr.tables.Scan(ctx, &tables)
	if err != nil {
		if errors.Is(storage.ErrTableNotFound, err) {
			return NewDatabaseShape(nil), nil
		}
		return nil, err
	}

	schemas := make([]*schema.Schema, 0, len(onlyTables))
	for _, t := range tables {
		if !slices.Contains(onlyTables, t.Table) {
			continue
		}
		// a bit hackish, but internal tables like indexes need to be filtered out for now
		if !t.Public() {
			continue
		}
		sch, err := sr.loadSchema(context.Background(), object.Table(t.Table))
		if err != nil {
			return nil, err
		}
		schemas = append(schemas, sch)
	}

	return NewDatabaseShape(schemas), nil
}
