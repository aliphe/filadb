package object

import (
	"fmt"
	"strings"
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

// Key gives the full path to a given column.
func Key(table Table, col string) string {
	if table != "" {
		return fmt.Sprintf("%s.%s", table, col)
	}
	return col
}

// ParseCol retrieves the col from an object Key.
func ParseCol(s string) string {
	parts := strings.Split(s, ".")
	if len(parts) == 1 {
		return parts[0]
	}
	return parts[1]
}
