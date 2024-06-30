package marshaler

import (
	"encoding/json"

	"github.com/aliphe/filadb/db/schema"
)

func toSchema(s *schema.Schema) string {
	sch := avroSchema{
		Type: "record",
		Name: string(s.Table),
	}
	for _, p := range s.Columns {
		sch.Fields = append(sch.Fields, avroField{
			Name: p.Name,
			Type: avroTypeMapper[p.Type],
		})
	}

	b, _ := json.Marshal(sch)
	return string(b)
}

type avroSchema struct {
	Type   string      `json:"type"`
	Name   string      `json:"name"`
	Fields []avroField `json:"fields"`
}

type avroField struct {
	Name string      `json:"name"`
	Type interface{} `json:"type"`
}

var avroTypeMapper = map[schema.ColumnType]string{
	schema.ColumnTypeText:   string(fieldTypeString),
	schema.ColumnTypeNumber: string(fieldTypeNumber),
}

var columnTypeMapper = map[string]schema.ColumnType{
	string(fieldTypeString): schema.ColumnTypeText,
	string(fieldTypeNumber): schema.ColumnTypeNumber,
}

type fieldType string

const (
	fieldTypeString fieldType = "string"
	fieldTypeNumber fieldType = "int"
)
