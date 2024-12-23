package index

import "github.com/aliphe/filadb/db/object"

type Index struct {
	Name    string
	Columns []string
}

func (i *Index) ObjectID() object.ID {
	return object.ID(i.Name)
}
