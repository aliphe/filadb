package table

import (
	"context"
	"fmt"

	"github.com/aliphe/filadb/db/errors"
	"github.com/aliphe/filadb/db/object"
)

func (q *Querier) Insert(ctx context.Context, row object.Row) error {
	b, err := q.marshaler.Marshal(row)
	if err != nil {
		return fmt.Errorf("validate data: %w", err)
	}
	id, ok := row["id"].(string)
	if !ok {
		return errors.RequiredPropertyError{Property: "id"}
	}

	err = q.store.Add(ctx, string(q.table), id, b)
	if err != nil {
		return fmt.Errorf("insert in table %s: %w", q.table, err)
	}

	return nil
}

func (q *Querier) Update(ctx context.Context, id string, row object.Row) error {
	b, err := q.marshaler.Marshal(row)
	if err != nil {
		return fmt.Errorf("validate data: %w", err)
	}

	err = q.store.Set(ctx, string(q.table), id, b)
	if err != nil {
		return fmt.Errorf("insert data: %w", err)
	}

	return nil
}
