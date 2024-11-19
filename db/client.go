package db

import (
	"context"

	"github.com/aliphe/filadb/db/object"
	"github.com/aliphe/filadb/db/schema"
	"github.com/aliphe/filadb/db/storage"
	"github.com/aliphe/filadb/db/table"
)

type readerWriter[T object.Identifiable] interface {
	Create(ctx context.Context, t T) error
	Get(ctx context.Context, id object.ID) (T, error)
}

type Client struct {
	store  storage.ReaderWriter
	schema readerWriter[*schema.Schema]
}

func NewClient(store storage.ReaderWriter, schema readerWriter[*schema.Schema]) *Client {
	c := &Client{
		store:  store,
		schema: schema,
	}

	return c
}

func (c *Client) Acquire(ctx context.Context, t object.Table) (*table.Querier[object.Row], error) {
	m, err := c.schema.Get(ctx, object.ID(t))
	if err != nil {
		return nil, err
	}

	q := table.NewQuerier[object.Row](c.store, m.Marshaler(), t)

	return q, nil
}

func (c *Client) CreateSchema(ctx context.Context, sch *schema.Schema) error {
	err := c.schema.Create(ctx, sch)
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) Shape(ctx context.Context, t object.Table) ([]string, error) {
	m, err := c.schema.Get(ctx, object.ID(t))
	if err != nil {
		return nil, err
	}

	return m.Marshaler().Shape(), nil
}
