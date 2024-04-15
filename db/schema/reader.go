package schema

import (
	"context"
	"errors"
	"fmt"

	"github.com/aliphe/filadb/db/storage"
	"github.com/linkedin/goavro/v2"
)

var (
	ErrSchemaNotFound = errors.New("schema not found")
)

type Reader struct {
	store storage.ReaderWriter
}

func NewReader(store storage.ReaderWriter) *Reader {
	return &Reader{
		store: store,
	}
}

func (r *Reader) Validator(ctx context.Context, table string) (*Validator, error) {
	s, ok, err := r.store.Get(ctx, string(InternalTableSchemas), table)
	if err != nil {
		return nil, fmt.Errorf("retrieve schema definition: %w", err)
	}
	if !ok {
		return nil, ErrSchemaNotFound
	}



	return &Validator{
		codec: *goavro.Codec,
	}
}
