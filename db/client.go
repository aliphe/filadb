package db

import (
	"context"

	"github.com/aliphe/filadb/db/object"
	"github.com/aliphe/filadb/db/schema"
	"github.com/aliphe/filadb/db/storage"
	"github.com/aliphe/filadb/db/table"
)

type schemaRegistry interface {
	Create(ctx context.Context, schema *schema.Schema) error
	Marshaler(ctx context.Context, table object.Table) (object.Marshaler, error)
}

type Client struct {
	store  storage.ReaderWriter
	schema schemaRegistry
}

func NewClient(store storage.ReaderWriter, schema schemaRegistry) *Client {
	c := &Client{
		store:  store,
		schema: schema,
	}

	return c
}

func (c *Client) Acquire(ctx context.Context, t object.Table) (*table.Querier[object.Row], error) {
	m, err := c.schema.Marshaler(ctx, t)
	if err != nil {
		return nil, err
	}

	q := table.NewQuerier[object.Row](c.store, m, t)

	return q, nil
}

func (c *Client) CreateSchema(ctx context.Context, sch *schema.Schema) error {
	err := c.schema.Create(ctx, sch)
	if err != nil {
		return err
	}

	return nil
}
