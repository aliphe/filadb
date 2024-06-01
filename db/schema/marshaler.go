package schema

import "github.com/aliphe/filadb/db/object"

type MarshalerFactory func(schema *Schema) Marshaler

type Marshaler interface {
	Marshal(obj object.Row) ([]byte, error)
	Unmarshal(s []byte) (object.Row, error)
	UnmarshalBatch(s [][]byte) ([]object.Row, error)
}
