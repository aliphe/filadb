package db

import (
	"context"
	"fmt"

	"github.com/aliphe/filadb/db/object"
	"github.com/aliphe/filadb/db/schema"
	"github.com/aliphe/filadb/db/storage"
)

type Client struct {
	schema *schema.ReaderWriter
	store  storage.ReaderWriter
}

func NewClient(store storage.ReaderWriter) *Client {
	return &Client{
		store:  store,
		schema: schema.NewReaderWriter(store),
	}
}

func (q *Client) Insert(ctx context.Context, tab, id string, row object.Row) error {
	b, err := q.schema.Marshal(ctx, tab, row)
	if err != nil {
		return fmt.Errorf("validate data: %w", err)
	}

	err = q.store.Add(ctx, tab, id, b)
	if err != nil {
		return fmt.Errorf("insert data: %w", err)
	}

	return nil
}

func (q *Client) Get(ctx context.Context, table, id string) (object.Row, bool, error) {
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

func (q *Client) Scan(ctx context.Context, table string) ([]object.Row, error) {
	d, err := q.store.Scan(ctx, table)
	if err != nil {
		return nil, err
	}

	s, err := q.schema.UnmarshalBatch(ctx, table, d)
	if err != nil {
		return nil, fmt.Errorf("unmarshal rows: %w", err)
	}

	return s, nil
}

func (q *Client) CreateSchema(ctx context.Context, sch schema.Schema) error {
	return q.schema.Create(ctx, &sch)
}
