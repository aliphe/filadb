package registry

import (
	"context"
	"errors"
	"fmt"

	"github.com/aliphe/filadb/db/schema"
	"github.com/aliphe/filadb/db/storage"
	"github.com/aliphe/filadb/db/table"
)

type Registry struct {
	store   storage.ReaderWriter
	indexes *table.Querier[internalTableIndexes]
	columns *table.Querier[internalTableIndexedColumns]
}

func New(store storage.ReaderWriter, factory schema.MarshalerFactory) (*Registry, error) {
	indexes := table.NewQuerier[internalTableIndexes](store, factory(internalTableIndexesSchema), internalTableIndexesName)
	columns := table.NewQuerier[internalTableIndexedColumns](store, factory(internalTableIndexedColumnsSchema), internalTableIndexedColumnsName)

	r := &Registry{
		store:   store,
		indexes: indexes,
		columns: columns,
	}

	err := r.load()
	if err != nil {
		return nil, fmt.Errorf("init index registry: %w", err)
	}

	return r, nil
}

func (r *Registry) load(ctx context.Context) error {
	ts := make([]internalTableIndexes, 0)
	err := r.indexes.Scan(ctx, &ts)
	if err != nil && !errors.Is(err, storage.ErrTableNotFound) {
		return err
	}

	for _, t := range ts {

	}

	return nil
}
