package schema

import (
	"encoding/json"
)

func toSchema(s *Schema) string {
	sch := avroSchema{
		Type: "record",
		Name: s.Table,
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

var avroTypeMapper = map[ColumnType]string{
	ColumnTypeText:   string(fieldTypeString),
	ColumnTypeNumber: string(fieldTypeNumber),
}

type fieldType string

const (
	fieldTypeString fieldType = "string"
	fieldTypeNumber fieldType = "int"
)
