package schema

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"

	"github.com/aliphe/filadb/db/storage"
)

type Writer struct {
	rw storage.ReaderWriter
}

func NewWriter(rw storage.ReaderWriter) *Writer {
	return &Writer{
		rw: rw,
	}
}

func (w *Writer) Create(ctx context.Context, schema *Schema) error {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(schema)
	if err != nil {
		return fmt.Errorf("encode schema: %w", err)
	}

	err = w.rw.Add(ctx, string(InternalTableSchemas), schema.Table, buf.Bytes())
	if err != nil {
		return fmt.Errorf("save schema: %w", err)
	}
	return nil
}
