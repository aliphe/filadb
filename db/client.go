package db

import (
	"context"

	"github.com/aliphe/filadb/db/errors"
	"github.com/aliphe/filadb/db/object"
	"github.com/aliphe/filadb/db/schema"
	"github.com/aliphe/filadb/db/storage"
	"github.com/aliphe/filadb/db/table"
)

type Client struct {
	store  storage.ReaderWriter
	tables map[object.Table]*table.Querier
	schema *schema.Admin
}

func NewClient(store storage.ReaderWriter, schema *schema.Admin) *Client {
	c := &Client{
		store:  store,
		schema: schema,
	}

	c.loadTables()

	return c
}

func (c *Client) loadTables() {
	schemas := c.schema.Marshalers()

	tables := make(map[object.Table]*table.Querier, len(schemas)+2)
	for t, s := range schemas {
		tables[t] = table.NewQuerier(c.store, s, t)
	}

	c.tables = tables
}

func (c *Client) Acquire(table object.Table) (*table.Querier, error) {
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
