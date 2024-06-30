package table

import (
	"context"
	"fmt"
)

func (q *Querier[T]) Insert(ctx context.Context, row T) error {
	b, err := q.marshaler.Marshal(row)
	if err != nil {
		return fmt.Errorf("validate data: %w", err)
	}
	err = q.store.Add(ctx, string(q.table), string(row.ObjectID()), b)
	if err != nil {
		return fmt.Errorf("insert in table %s: %w", q.table, err)
	}

	return nil
}

func (q *Querier[T]) Update(ctx context.Context, row T) error {
	b, err := q.marshaler.Marshal(row)
	if err != nil {
		return fmt.Errorf("validate data: %w", err)
	}

	err = q.store.Set(ctx, string(q.table), string(row.ObjectID()), b)
	if err != nil {
		return fmt.Errorf("insert data: %w", err)
	}

	return nil
}
