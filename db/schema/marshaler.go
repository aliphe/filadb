package schema

import (
	"context"
	"fmt"

	"github.com/aliphe/filadb/db/object"
	"github.com/aliphe/filadb/db/storage"
	"github.com/aliphe/filadb/pkg/avro"
)

type avroMarshaler struct {
	schema string
}

func newMarshaler(ctx context.Context, r storage.Reader, table string) (*avroMarshaler, int32, error) {
	sch, err := fromStorage(ctx, r, table)
	if err != nil {
		return nil, -1, err
	}
	return &avroMarshaler{
		schema: toSchema(sch),
	}, sch.version, nil
}

func (a *avroMarshaler) Marshal(obj object.Row) ([]byte, error) {
	return avro.Marshal(a.schema, obj)
}

func (a *avroMarshaler) Unmarshal(b []byte) (object.Row, error) {
	return avro.Unmarshal(a.schema, b)
}

func (a *avroMarshaler) UnmarshalBatch(s [][]byte) ([]object.Row, error) {
	out := make([]object.Row, 0, len(s))

	for _, r := range s {
		o, err := avro.Unmarshal(a.schema, r)
		if err != nil {
			return nil, fmt.Errorf("marshall data: %w", err)
		}

		out = append(out, o)
	}
	return out, nil
}
