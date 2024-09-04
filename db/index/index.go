package index

import (
	"strings"

	"github.com/aliphe/filadb/db/object"
)

type ID string

// Index represents an index in the database, allowing optimized queries on specific objects.
type Index struct {
	// Table represents the table on which the index is applied.
	Table object.Table

	// Columns represents the columns that are indexed.
	Columns []string
}

func New(table object.Table, cols []string) *Index {
	cpy := make([]string, len(cols))
	copy(cpy, cols)

	return &Index{
		Table:   table,
		Columns: cpy,
	}
}

func (i *Index) ID() ID {
	return ID(strings.Join(i.Columns, "_"))
}

func (i *Index) Find()

// Key represents the indexed key of a given row
type Key string

type IndexedRow struct {
	Index ID
	Key   Key
	Row   object.ID
}
