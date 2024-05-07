package schema

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"

	"github.com/aliphe/filadb/db/object"
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

func (r *Reader) Marshal(ctx context.Context, schema string, obj object.Row) ([]byte, error) {
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

func (r *Reader) Unmarshal(ctx context.Context, schema string, b []byte) (object.Row, error) {
	sch, err := r.schema(ctx, schema)
	if err != nil {
		return nil, err
	}

	out, err := avro.Unmarshal(sch, b)
	if err != nil {
		return nil, fmt.Errorf("marshall data: %w", err)
	}

	return out, nil
}

func (r *Reader) UnmarshalBatch(ctx context.Context, schema string, b [][]byte) ([]object.Row, error) {
	sch, err := r.schema(ctx, schema)
	if err != nil {
		return nil, err
	}

	return r.unmarshalBatch(sch, b)
}

func (r *Reader) unmarshalBatch(schema string, b [][]byte) ([]object.Row, error) {
	out := make([]object.Row, 0, len(b))

	for _, r := range b {
		o, err := avro.Unmarshal(schema, r)
		if err != nil {
			return nil, fmt.Errorf("marshall data: %w", err)
		}

		out = append(out, o)
	}
	return out, nil

}

func (r *Reader) schema(ctx context.Context, schema string) (string, error) {
	s, ok, err := r.reader.Get(ctx, string(InternalTableSchemas), schema)
	if err != nil {
		return "", fmt.Errorf("retrieve schema definition: %w", err)
	}
	if !ok {
		return "", ErrSchemaNotFound
	}

	enc := gob.NewDecoder(bytes.NewReader(s))

	var sch Schema
	err = enc.Decode(&sch)
	if err != nil {
		return "", fmt.Errorf("decode schema: %w", err)
	}

	return toSchema(&sch), nil
}
