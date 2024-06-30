package table

import (
	"context"
	"fmt"

	"github.com/aliphe/filadb/db/object"
)

func (q *Querier[T]) Get(ctx context.Context, id object.ID, dst *T) (bool, error) {
	d, ok, err := q.store.Get(ctx, string(q.table), string(id))
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}

	err = q.marshaler.Unmarshal(d, dst)
	if err != nil {
		return true, fmt.Errorf("unmarshal row: %w", err)
	}

	return true, nil
}

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
