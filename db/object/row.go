package object

import (
	"fmt"
)

type ID string

type Row map[string]interface{}

func (r Row) ObjectID() ID {
	return ID(fmt.Sprintf("%v", r["id"]))
}

func (r Row) ObjectTable() Table {
	// TODO check this
	table, _ := r["table"].(string)
	return Table(table)
}

type Table string

type Identifiable interface {
	ObjectID() ID
	ObjectTable() Table
}
