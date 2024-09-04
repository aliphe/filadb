package registry

import (
	"context"
	"errors"
	"fmt"

	"github.com/aliphe/filadb/db/index"
	"github.com/aliphe/filadb/db/object"
	"github.com/aliphe/filadb/db/schema"
	"github.com/aliphe/filadb/db/storage"
	"github.com/aliphe/filadb/db/table"
)

type Registry struct {
	store      storage.ReaderWriter
	idxQuerier *table.Querier[internalTableIndexes]
	colQuerier *table.Querier[internalTableIndexedColumns]
	indexes    map[object.Table][]*index.Index
}

func New(store storage.ReaderWriter, factory schema.MarshalerFactory) (*Registry, error) {
	indexes := table.NewQuerier[internalTableIndexes](store, factory(internalTableIndexesSchema), nil, internalTableIndexesName)
	columns := table.NewQuerier[internalTableIndexedColumns](store, factory(internalTableIndexedColumnsSchema), nil, internalTableIndexedColumnsName)

	r := &Registry{
		store:      store,
		idxQuerier: indexes,
		colQuerier: columns,
	}

	err := r.load(context.Background())
	if err != nil {
		return nil, fmt.Errorf("init index registry: %w", err)
	}

	return r, nil
}

func (r *Registry) load(ctx context.Context) error {
	ts := make([]internalTableIndexes, 0)
	err := r.idxQuerier.Scan(ctx, &ts)
	if err != nil && !errors.Is(err, storage.ErrTableNotFound) {
		return err
	}

	for _, t := range ts {
		cs := make([]internalTableIndexedColumns, 0)
		err := r.colQuerier.Scan(ctx, &cs)
		if err != nil {
			return err
		}
		cols := make([]string, 0, len(cs))
		for _, c := range cs {
			cols = append(cols, c.Column)
		}

		r.indexes[t.Table] = append(r.indexes[t.Table], index.New(t.Table, cols))
	}

	return nil
}

func (r *Registry) Indexes(table object.Table) []*index.Index {
	idxs, ok := r.indexes[table]
	if !ok {
		return nil
	}

	return idxs
}
