package avro

import (
	"fmt"

	"github.com/aliphe/filadb/db/object"
	"github.com/aliphe/filadb/db/schema"
	"github.com/aliphe/filadb/pkg/avro"
)

type marshaler struct {
	schema string
}

func NewMarshaler(schema *schema.Schema) schema.Marshaler {
	return &marshaler{
		schema: toSchema(schema),
	}
}

func (a *marshaler) Marshal(obj object.Row) ([]byte, error) {
	return avro.Marshal(a.schema, obj)
}

func (a *marshaler) Unmarshal(b []byte) (object.Row, error) {
	return avro.Unmarshal(a.schema, b)
}

func (a *marshaler) UnmarshalBatch(s [][]byte) ([]object.Row, error) {
	out := make([]object.Row, 0, len(s))

	for _, r := range s {
		o, err := avro.Unmarshal(a.schema, r)
		if err != nil {
			return nil, fmt.Errorf("unmarshall data: %w", err)
		}

		out = append(out, o)
	}
	return out, nil
}
