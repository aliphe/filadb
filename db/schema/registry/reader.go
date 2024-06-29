package registry

import (
	"context"

	"github.com/aliphe/filadb/db/object"
	"github.com/aliphe/filadb/db/schema"
)

func (a *Registry) fromStorage(ctx context.Context, table object.Table) (schema.Marshaler, error) {
	out := schema.Schema{
		Table: table,
	}
	cols, err := a.columns.Scan(ctx)
	if err != nil {
		return nil, err
	}
	for _, c := range cols {
		if c["table"] == string(table) {
			t, ok := c["type"].(string)
			if !ok {
				t = string(schema.ColumnTypeText)
			}
			out.Columns = append(out.Columns, schema.Column{
				Name: c["column"].(string),
				Type: schema.ColumnType(t),
			})
		}
	}

	return a.factory(&out), nil
}
