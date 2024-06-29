package table

import (
	"context"
	"fmt"

	"github.com/aliphe/filadb/db/object"
)

type marshaler interface {
	Marshal(obj object.Row) ([]byte, error)
	Unmarshal(b []byte) (object.Row, error)
	UnmarshalBatch(b [][]byte) ([]object.Row, error)
}

func (q *Querier) Get(ctx context.Context, id object.ID) (object.Row, bool, error) {
	d, ok, err := q.store.Get(ctx, string(q.table), string(id))
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}

	s, err := q.marshaler.Unmarshal(d)
	if err != nil {
		return nil, true, fmt.Errorf("unmarshal row: %w", err)
	}

	return s, true, nil
}

func (q *Querier) Scan(ctx context.Context) ([]object.Row, error) {
	d, err := q.store.Scan(ctx, string(q.table))
	if err != nil {
		return nil, err
	}

	s, err := q.marshaler.UnmarshalBatch(d)
	if err != nil {
		return nil, fmt.Errorf("unmarshal rows: %w", err)
	}

	return s, nil
}
