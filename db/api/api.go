package api

import (
	"context"
	"fmt"

	"github.com/aliphe/filadb/db/data"
	"github.com/aliphe/filadb/db/schema"
)

type Querier struct {
	schema schema.ReaderWriter
	data   data.DB
}

func New(schema schema.ReaderWriter, data data.DB) *Querier {
	return &Querier{
		schema: schema,
		data:   data,
	}
}

func (q *Querier) Insert(ctx context.Context, table, id string, data interface{}) error {
	v, err := q.schema.Validator(ctx, table)
	if err != nil {
		return fmt.Errorf("create in table %s: %w", table, err)
	}

	b, err := v.Marshall(data)
	if err != nil {
		return fmt.Errorf("invalid data: %w", err)
	}

	err = q.data.Set(ctx, table, id, b)
	if err != nil {
		return fmt.Errorf("insert data: %w", err)
	}

	return nil
}

func (q *Querier) Get(ctx context.Context, table, id string) (interface{}, bool, error) {
	v, err := q.schema.Validator(ctx, table)
	if err != nil {
		return nil, false, fmt.Errorf("get in table %s: %w", table, err)
	}

	d, ok, err := q.data.Get(ctx, table, id)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}

	out, err := v.Unmarshall(d)
	if err != nil {
		return nil, false, fmt.Errorf("unmarshall object: %w", err)
	}

	return out, true, nil
}
