package db

import (
	"context"

	"github.com/aliphe/filadb/db/errors"
	"github.com/aliphe/filadb/db/object"
	"github.com/aliphe/filadb/db/schema"
	"github.com/aliphe/filadb/db/storage"
	"github.com/aliphe/filadb/db/table"
)

type schemaRegistry interface {
	Create(ctx context.Context, schema *schema.Schema) error
	Marshalers() map[object.Table]object.Marshaler
}

type Client struct {
	store  storage.ReaderWriter
	tables map[object.Table]*table.Querier[object.Row]
	schema schemaRegistry
}

func NewClient(store storage.ReaderWriter, schema schemaRegistry) *Client {
	c := &Client{
		store:  store,
		schema: schema,
	}

	c.loadTables()

	return c
}

func (c *Client) loadTables() {
	schemas := c.schema.Marshalers()

	tables := make(map[object.Table]*table.Querier[object.Row], len(schemas))
	for t, s := range schemas {
		tables[t] = table.NewQuerier[object.Row](c.store, s, t)
	}

	c.tables = tables
}

func (c *Client) Acquire(table object.Table) (*table.Querier[object.Row], error) {
	q, ok := c.tables[table]
	if !ok {
		return nil, errors.ErrTableNotFound
	}

	return q, nil
}

func (c *Client) CreateSchema(ctx context.Context, sch *schema.Schema) error {
	err := c.schema.Create(ctx, sch)
	if err != nil {
		return err
	}

	c.loadTables()

	return nil
}
