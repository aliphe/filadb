package db

import (
	"context"
	"fmt"

	"github.com/aliphe/filadb/db/index"
	"github.com/aliphe/filadb/db/object"
	"github.com/aliphe/filadb/db/schema"
	"github.com/aliphe/filadb/db/storage"
	"github.com/aliphe/filadb/db/system"
)

type schemaStore interface {
	Create(ctx context.Context, sch *schema.Schema) error
	Get(ctx context.Context, table object.Table) (*schema.Schema, error)
	Shape(ctx context.Context, tables []object.Table) (*system.DatabaseShape, error)
}

type indexStore interface {
	Scan(ctx context.Context, table object.Table) ([]*index.Index, error)
	Create(ctx context.Context, idx *index.Index) error
	Index(ctx context.Context, idx *index.Index, rows ...object.Row) error
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

type Op int

const (
	OpEqual = iota + 1
	OpLessThan
	OpLessThanEqual
	OpMoreThan
	OpMoreThanEqual
	OpInclude
)

type Filter struct {
	Col string
	Op  Op
	Val any
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
		return fmt.Errorf("fetch indexes: %w", err)
	}

	for _, idx := range idxs {
		err = c.index.Index(ctx, idx, r)
		if err != nil {
			return fmt.Errorf("index row: %w", err)
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

func (c *Client) Scan(ctx context.Context, t object.Table, dst *[]object.Row, filters ...Filter) error {
	sch, err := c.schema.Get(ctx, t)
	if err != nil {
		return err
	}

	s, err := c.indexScan(ctx, t, filters...)
	if err != nil {
		return err
	}
	// fallback to full scan
	if len(s) == 0 {
		s, err = c.store.Scan(ctx, string(t))
		if err != nil {
			return nil
		}
	}

	err = sch.Marshaler().UnmarshalBatch(s, dst)
	if err != nil {
		return err
	}

	return nil
}

// indexScan will attempt to fetch the rows using indexes defined on the table.
// if none are usable, it will return an empty slice and empty error.
func (c *Client) indexScan(ctx context.Context, t object.Table, filters ...Filter) ([][]byte, error) {
	idxs, err := c.index.Scan(ctx, t)
	if err != nil {
		return nil, err
	}

	cols := make([]string, 0, len(filters))
	for _, f := range filters {
		cols = append(cols, f.Col)
	}

	var idx *index.Index
	for _, i := range idxs {
		if i.Matches(cols) {
			idx = i
			break
		}
	}
	if idx == nil {
		return nil, nil
	}

	row := make(object.Row)
	for _, f := range filters {
		row[f.Col] = f.Val
	}

	ids, err := c.store.Get(ctx, string(idx.Name), string(idx.Key(row)))
	if err != nil {
		return nil, err
	}
	out := make([][]byte, 0, len(ids))
	// get each row with GetRow
	for _, id := range ids {
		s, err := c.store.Get(ctx, string(t), string(id))
		if err != nil {
			return nil, err
		}
		out = append(out, s[0])
	}

	return out, nil
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

func (c *Client) GetSchema(ctx context.Context, t object.Table) (*schema.Schema, error) {
	sch, err := c.schema.Get(ctx, t)
	if err != nil {
		return nil, err
	}

	return sch, nil
}

// Index functions
func (c *Client) CreateIndex(ctx context.Context, idx *index.Index) error {
	err := c.index.Create(ctx, idx)
	if err != nil {
		return fmt.Errorf("create index: %w", err)
	}

	var rows []object.Row
	err = c.Scan(ctx, idx.Table, &rows)
	if err != nil {
		return fmt.Errorf("retrieve rows to index: %w", err)
	}

	err = c.index.Index(ctx, idx, rows...)
	if err != nil {
		return fmt.Errorf("index rows: %w", err)
	}

	return nil
}

func (c *Client) Shape(ctx context.Context, tables []object.Table) (*system.DatabaseShape, error) {
	return c.schema.Shape(ctx, tables)
}
