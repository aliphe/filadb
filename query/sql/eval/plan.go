package eval

import (
	"slices"

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

func plan(f parser.From) (step, error) {
	// order the joins by dependency, making sure the most depended on node is leftmost
	//
	// build the tree based on the dependencies
	//
	var queries []query
	queries = append(queries, query{
		table:   f.Table,
		filters: f.Where,
	})

	for _, j := range f.Joins {
		queries = append(queries, query{
			table:   j.Table,
			filters: j.On,
		})
	}

	slices.SortFunc(queries, func(a, b query) int {
		return -1
	})

	return step{}, nil
}
