package schema

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/aliphe/filadb/db/storage"
	"github.com/linkedin/goavro/v2"
)

//go:embed schema.json
var avroSchema string

type Writer struct {
	codec *goavro.Codec
	store storage.ReaderWriter
}

func NewWriter(store storage.ReaderWriter) (*Writer, error) {
	c, err := goavro.NewCodec(avroSchema)
	if err != nil {
		return nil, fmt.Errorf("load avro codec: %w", err)
	}

	return &Writer{
		codec: c,
		store: store,
	}, nil
}

func (w *Writer) Create(ctx context.Context, schema *Schema) error {
	sch := native(schema)
	b, err := w.codec.BinaryFromNative(nil, sch)
	if err != nil {
		return fmt.Errorf("encode schema: %w", err)
	}

	err = w.store.Add(ctx, string(InternalTableSchemas), schema.Table, b)
	if err != nil {
		return fmt.Errorf("save schema: %w", err)
	}
	return nil
}

func native(schema *Schema) map[string]interface{} {
	properties := make([]interface{}, 0, len(schema.Properties))
	for _, p := range schema.Properties {
		properties = append(properties, map[string]interface{}{
			"name":       p.Name,
			"type":       string(p.Type),
			"primaryKey": p.PrimaryKey,
		})
	}
	return map[string]interface{}{
		"name":       schema.Table,
		"properties": properties,
	}
}
