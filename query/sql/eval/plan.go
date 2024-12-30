package eval

import (
	"github.com/aliphe/filadb/db/object"
	"github.com/aliphe/filadb/query/sql/parser"
)

type step struct {
	queries  []query
	children []step
}

type query struct {
	table   object.Table
	filters []parser.Filter
}
