package db

import (
	"context"

	"github.com/aliphe/filadb/db/index"
	"github.com/aliphe/filadb/db/object"
	"github.com/aliphe/filadb/db/schema"
	"github.com/aliphe/filadb/db/storage"
)

type schemaStore interface {
	Create(ctx context.Context, sch *schema.Schema) error
	Get(ctx context.Context, table object.Table) (*schema.Schema, error)
}

type indexStore interface {
	Scan(ctx context.Context, table object.Table) ([]*index.Index, error)
	Create(ctx context.Context, idx *index.Index) error
}

type Client struct {
	store  storage.ReaderWriter
	schema schemaStore
	index  indexStore
}

func NewClient(store storage.ReaderWriter, schema schemaStore, index indexStore) *Client {
	c := &Client{
		store:  store,
		schema: schema,
		index:  index,
	}

	return c
}

//
// Row operations
//

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

	idxs, err := c.index.Scan(ctx, t)
	if err != nil {
		return err
	}

	for _, idx := range idxs {
		key := idx.Key(r)
		err := c.store.Add(ctx, string(idx.Name), string(key), []byte(r.ObjectID()))
		if err != nil {
			return err
		}
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

// GetRow gets a row given the provided ID.
// It will be deprecated as soon as indexes get first-class support.
func (c *Client) GetRow(ctx context.Context, t object.Table, id object.ID, dst *object.Row) error {
	sch, err := c.schema.Get(ctx, t)
	if err != nil {
		return err
	}

	bs, err := c.store.Get(ctx, string(t), string(id))
	if err != nil {
		return err
	}
	// if we did not find the row, or found many for the given id, return nothing
	// the case "more than one" should not happen
	if len(bs) != 1 {
		return nil
	}

	err = sch.Marshaler().Unmarshal(bs[0], dst)
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

	// scan table indexes
	//
	// find best one (heuristic TBD)
	//
	// fetch ids from index (my btree does not handle multi values on same key rn)
	//
	// get each row with GetRow

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

// Index functions
func (c *Client) CreateIndex(ctx context.Context, idx *index.Index) error {
	err := c.index.Create(ctx, idx)
	if err != nil {
		return err
	}

	return nil
}
