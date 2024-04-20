package schema

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"

	"github.com/aliphe/filadb/db/storage"
	"github.com/aliphe/filadb/pkg/avro"
)

var (
	ErrSchemaNotFound = errors.New("schema not found")
)

type Reader struct {
	reader storage.Reader
}

func NewReader(reader storage.Reader) *Reader {
	return &Reader{
		reader: reader,
	}
}

func (r *Reader) Marshal(ctx context.Context, schema string, obj interface{}) ([]byte, error) {
	s, ok, err := r.reader.Get(ctx, string(InternalTableSchemas), schema)
	if err != nil {
		return nil, fmt.Errorf("retrieve schema definition: %w", err)
	}
	if !ok {
		return nil, ErrSchemaNotFound
	}

	dec := gob.NewDecoder(bytes.NewReader(s))

	var sch Schema
	err = dec.Decode(&sch)
	if err != nil {
		return nil, fmt.Errorf("decode schema: %w", err)
	}

	b, err := avro.Marshal(toSchema(&sch), obj)
	if err != nil {
		return nil, fmt.Errorf("marshall data: %w", err)
	}

	return b, nil
}

func (r *Reader) Unmarshal(ctx context.Context, schema string, b []byte) (interface{}, error) {
	s, ok, err := r.reader.Get(ctx, string(InternalTableSchemas), schema)
	if err != nil {
		return nil, fmt.Errorf("retrieve schema definition: %w", err)
	}
	if !ok {
		return nil, ErrSchemaNotFound
	}

	enc := gob.NewDecoder(bytes.NewReader(s))

	var sch Schema
	err = enc.Decode(&sch)
	if err != nil {
		return nil, fmt.Errorf("decode schema: %w", err)
	}

	out, err := avro.Unmarshal(toSchema(&sch), b)
	if err != nil {
		return nil, fmt.Errorf("marshall data: %w", err)
	}

	return out, nil
}
