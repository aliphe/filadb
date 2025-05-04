package object

import (
	"fmt"
)

type ID string

type Row map[string]any

func (r Row) ObjectID() ID {
	return ID(fmt.Sprintf("%v", r["id"]))
}

type Table string

type Identifiable interface {
	ObjectID() ID
}

func Key(table Table, col string) string {
	if table != "" {
		return fmt.Sprintf("%s.%s", table, col)
	}
	return col
}
