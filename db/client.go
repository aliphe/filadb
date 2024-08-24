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
	Querier(ctx context.Context, table object.Table) (*table.Querier[object.Row], error)
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

func (c *Client) Acquire(ctx context.Context, table object.Table) (*table.Querier[object.Row], error) {
	q, err := c.schema.Querier(ctx, table)
	if err != nil {
		return nil, err
	}

	return q, nil
}

func (c *Client) CreateSchema(ctx context.Context, sch *schema.Schema) error {
	err := c.schema.Create(ctx, sch)
	if err != nil {
		return err
	}

	return nil
}
