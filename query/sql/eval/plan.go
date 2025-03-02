package eval

import (
	"context"

	"github.com/aliphe/filadb/db/object"
	"github.com/aliphe/filadb/query/sql/parser"
)

type Plan struct {
	Batches []Batch
}

type Batch struct {
	Steps []Step
}

type Step struct {
	Table   object.Table
	Filters []parser.Filter
}

type Planner interface {
	Plan(ctx context.Context, from object.Table, filters []parser.Filter, joins []parser.Join) (*Plan, error)
}
