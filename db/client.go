package db

import (
	"context"

	idxregistry "github.com/aliphe/filadb/db/index/registry"
	"github.com/aliphe/filadb/db/object"
	"github.com/aliphe/filadb/db/schema"
	schregistry "github.com/aliphe/filadb/db/schema/registry"
	"github.com/aliphe/filadb/db/storage"
	"github.com/aliphe/filadb/db/table"
)

type Client struct {
	store  storage.ReaderWriter
	schema *schregistry.Registry
	index  *idxregistry.Registry
}

func NewClient(store storage.ReaderWriter, schema *schregistry.Registry, index *idxregistry.Registry) *Client {
	c := &Client{
		store:  store,
		schema: schema,
		index:  index,
	}

	return c
}

func (c *Client) Acquire(ctx context.Context, t object.Table) (*table.Querier[object.Row], error) {
	m, err := c.schema.Marshaller(ctx, t)
	if err != nil {
		return nil, err
	}

	idxs := c.index.Indexes(t)

	q := table.NewQuerier[object.Row](c.store, m, idxs, t)

	return q, nil
}

func (c *Client) CreateSchema(ctx context.Context, sch *schema.Schema) error {
	err := c.schema.Create(ctx, sch)
	if err != nil {
		return err
	}

	return nil
}
