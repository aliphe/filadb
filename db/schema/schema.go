package schema

import (
	"fmt"

	"github.com/aliphe/filadb/db/storage"
)

type Schema struct {
	Table      string
	Properties []*Property
}

type Property struct {
	Name       string
	PrimaryKey bool
	Type       PropertyType
}

type PropertyType string

const (
	PropertyTypeText     PropertyType = "text"
	PropertyTypeNumber   PropertyType = "number"
	PropertyTypeDateTime PropertyType = "datetime"
)

type InternalTable string

const (
	InternalTableSchemas InternalTable = "schemas"
)

type ReaderWriter struct {
	Reader
	Writer
}

func NewReaderWriter(store storage.ReaderWriter) (*ReaderWriter, error) {
	w, err := NewWriter(store)
	if err != nil {
		return nil, fmt.Errorf("create writer: %w", err)
	}
	r := NewReader(store)

	return &ReaderWriter{
		Reader: *r,
		Writer: *w,
	}, nil
}
