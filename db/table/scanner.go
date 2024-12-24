package table

import (
	"context"
	"fmt"
)

func (q *Querier[T]) Scan(ctx context.Context, dest *[]T) error {
	d, err := q.store.Scan(ctx, string(q.table))
	if err != nil {
		return err
	}

	err = q.marshaler.UnmarshalBatch(d, dest)
	if err != nil {
		return fmt.Errorf("unmarshal rows: %w", err)
	}

	return nil
}
