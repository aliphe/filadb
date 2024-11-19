package object

import (
	"fmt"
)

type ID string

type Row map[string]interface{}

func (r Row) ObjectID() ID {
	return ID(fmt.Sprintf("%v", r["id"]))
}

type Table string

type Identifiable interface {
	ObjectID() ID
}
