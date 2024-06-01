package schema

import (
	"context"
	"fmt"

	"github.com/aliphe/filadb/db/object"
)

func (a *Admin) fromStorage(ctx context.Context, table object.Table) (Marshaler, error) {
	t, ok, err := a.tables.Get(ctx, string(table))
	if err != nil {
		return nil, fmt.Errorf("retrieve table information: %w", err)
	}
	if !ok {
		return nil, ErrTableNotFound
	}

	v, ok := t["version"].(int32)
	if !ok {
		return nil, fmt.Errorf("internal error")
	}

	out := Schema{
		Table:   table,
		version: v,
	}
	cols, err := a.columns.Scan(ctx)
	for _, c := range cols {
		if c["table"] == string(table) {
			t, ok := c["type"].(string)
			if !ok {
				t = string(ColumnTypeText)
			}
			out.Columns = append(out.Columns, Column{
				Name: c["column"].(string),
				Type: ColumnType(t),
			})
		}
	}

	return a.factory(&out), nil
}
