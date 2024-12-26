package index

import (
	"crypto/md5"
	"encoding/base64"
	"fmt"

	"github.com/aliphe/filadb/db/object"
)

type Index struct {
	Table object.Table

	Name string

	Columns []string
}

func New(table object.Table, columns ...string) *Index {
	return &Index{
		Table:   table,
		Columns: columns,
	}
}

// Key represents an index key based on a given row
type Key string

// Key builds the index key based on the properties of the given row
func (i *Index) Key(row object.Row) Key {
	var key string
	for _, c := range i.Columns {
		key += fmt.Sprintf("%s=%v,", c, row[c])
	}

	b := md5.Sum([]byte(key))

	str := base64.StdEncoding.EncodeToString(b[:])

	return Key(str)
}
