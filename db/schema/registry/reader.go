package registry

import (
	"context"

	"github.com/aliphe/filadb/db/object"
	"github.com/aliphe/filadb/db/schema"
)

func (a *Registry) fromStorage(ctx context.Context, table object.Table) (object.Marshaler, error) {
	out := schema.Schema{
		Table: table,
	}
	cols := make([]internalTableColumns, 0)
	err := a.columns.Scan(ctx, &cols)
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

	return a.factory(&out), nil
}
