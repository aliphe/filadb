package schema

import "github.com/aliphe/filadb/db/object"

type MarshalerFactory func(*Schema) object.Marshaler

type Schema struct {
	Table   object.Table
	Columns []Column
}

func (s *Schema) Marshaler() object.Marshaler {
	return &marshaler{
		src:    s,
		schema: toSchema(s),
	}
}

func (s *Schema) ObjectID() object.ID {
	return object.ID(s.Table)
}

type Column struct {
	Name string
	Type ColumnType
}

type ColumnType string

const (
	ColumnTypeText   ColumnType = "text"
	ColumnTypeNumber ColumnType = "number"
)
