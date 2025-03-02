package planner

import (
	"context"

	"github.com/aliphe/filadb/db/object"
	"github.com/aliphe/filadb/query/sql/eval"
	"github.com/aliphe/filadb/query/sql/parser"
)

var _ eval.Planner = (*Stateless)(nil)

type Stateless struct {
}

func (s *Stateless) Plan(ctx context.Context, table object.Table, filters []parser.Filter, joins []parser.Join) (*eval.Plan, error) {
	return nil, nil
}
