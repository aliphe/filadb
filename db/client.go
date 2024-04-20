package db

import (
	"context"
	"fmt"

	"github.com/aliphe/filadb/db/schema"
	"github.com/aliphe/filadb/db/storage"
	"github.com/aliphe/filadb/db/table"
)

type Client struct {
	schema schema.Reader
	store  storage.ReaderWriter
}

func NewClient(store storage.ReaderWriter, schema schema.Reader) *Client {
	return &Client{
		store:  store,
		schema: schema,
	}
}

func (q *Client) Insert(ctx context.Context, tab, id string, data interface{}) error {
	b, err := q.schema.Marshal(ctx, tab, data)
	if err != nil {
		return fmt.Errorf("validate data: %w", err)
	}

	writer := table.NewStore(q.store)

	err = writer.Set(ctx, tab, id, b)
	if err != nil {
		return fmt.Errorf("insert data: %w", err)
	}

	return nil
}

func (q *Client) Get(ctx context.Context, table, id string) (interface{}, bool, error) {
	d, ok, err := q.store.Get(ctx, table, id)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}

	s, err := q.schema.Unmarshal(ctx, table, d)
	if err != nil {
		return nil, true, fmt.Errorf("unmarshal row: %w", err)
	}
	return s, true, nil
}
