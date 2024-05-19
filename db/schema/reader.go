package schema

import (
	"context"
	"fmt"

	"github.com/aliphe/filadb/db/storage"
	"github.com/aliphe/filadb/pkg/avro"
)

func fromStorage(ctx context.Context, r storage.Reader, table string) (*Schema, error) {
	t, ok, err := r.Get(ctx, string(InternalTableTables), table)
	if err != nil {
		return nil, fmt.Errorf("retrieve table information: %w", err)
	}
	if !ok {
		return nil, ErrTableNotFound
	}

	b, err := avro.Unmarshal(toSchema(&internalTableTablesSchema), t)
	if err != nil {
		return nil, fmt.Errorf("unmarshal internal table schema: %w", err)
	}
	v, ok := b["version"].(int32)
	if !ok {
		return nil, fmt.Errorf("internal error")
	}

	out := Schema{
		Table:   table,
		version: v,
	}
	cols, err := r.Scan(ctx, string(InternalTableColumns))
	for _, c := range cols {
		b, err := avro.Unmarshal(toSchema(&internalTableColumnsSchema), c)
		if err != nil {
			return nil, fmt.Errorf("unmarshal internal columns schema: %w", err)
		}
		if b["table"] == table {
			out.Columns = append(out.Columns, Column{
				Name: b["column"].(string),
				Type: b["type"].(ColumnType),
			})
		}
	}

	return &out, nil
}
