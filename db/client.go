package db

import (
	"context"

	"github.com/aliphe/filadb/db/index"
	"github.com/aliphe/filadb/db/object"
	"github.com/aliphe/filadb/db/schema"
	"github.com/aliphe/filadb/db/storage"
)

type schemaReaderWriter interface {
	Create(ctx context.Context, sch *schema.Schema) error
	Get(ctx context.Context, table object.Table) (*schema.Schema, error)
}

type indexScanner interface {
	Scan(ctx context.Context, t object.Table, cols ...string) ([]index.Index, error)
}

type Client struct {
	store  storage.ReaderWriter
	schema schemaReaderWriter
	index  indexScanner
}

func NewClient(store storage.ReaderWriter, schema schemaReaderWriter, index indexScanner) *Client {
	c := &Client{
		store:  store,
		schema: schema,
		index:  index,
	}

	return c
}

func (c *Client) InsertRow(ctx context.Context, t object.Table, r object.Row) error {
	sch, err := c.schema.Get(ctx, t)
	if err != nil {
		return err
	}

	b, err := sch.Marshaler().Marshal(r)
	if err != nil {
		return err
	}

	err = c.store.Add(ctx, string(t), string(r.ObjectID()), b)
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) UpdateRow(ctx context.Context, t object.Table, r object.Row) error {
	sch, err := c.schema.Get(ctx, t)
	if err != nil {
		return err
	}

	b, err := sch.Marshaler().Marshal(r)
	if err != nil {
		return err
	}

	err = c.store.Set(ctx, string(t), string(r.ObjectID()), b)
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) GetRow(ctx context.Context, t object.Table, id object.ID, dst *object.Row) error {
	sch, err := c.schema.Get(ctx, t)
	if err != nil {
		return err
	}

	bs, ok, err := c.store.Get(ctx, string(t), string(id))
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	err = sch.Marshaler().Unmarshal(bs, dst)
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) Scan(ctx context.Context, t object.Table, dst *[]object.Row) error {
	sch, err := c.schema.Get(ctx, t)
	if err != nil {
		return err
	}

	s, err := c.store.Scan(ctx, string(t))
	if err != nil {
		return nil
	}

	err = sch.Marshaler().UnmarshalBatch(s, dst)
	if err != nil {
		return err
	}

	return nil
}

/**
* Schema functions
 */
func (c *Client) CreateSchema(ctx context.Context, sch *schema.Schema) error {
	err := c.schema.Create(ctx, sch)
	if err != nil {
		return err
	}

	return nil
}
func (c *Client) Shape(ctx context.Context, t object.Table) ([]string, error) {
	sch, err := c.schema.Get(ctx, t)
	if err != nil {
		return nil, err
	}

	return sch.Marshaler().Shape(), nil
}
