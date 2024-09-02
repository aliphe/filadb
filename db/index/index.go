package index

import "github.com/aliphe/filadb/db/object"

type ID string

// Index represents an index in the database, allowing optimized queries on specific objects.
type Index struct {
	// ID uniquely identifies the index on a given table.
	ID string

	// Table represents the table on which the index is stored.
	Table object.Table

	// Keyer is the function used to generate a key for this index, used to find the row.
	Keyer Keyer
}

// Key represents the indexed key of a given row
type Key string

type Keyer func(object.Row) string
