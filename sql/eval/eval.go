package eval

import (
	"context"
	"fmt"

	"github.com/aliphe/filadb/db"
	"github.com/aliphe/filadb/db/object"
	"github.com/aliphe/filadb/sql/parser"
)

type Evaluator struct {
	client *db.Client
}

func New(client *db.Client) *Evaluator {
	return &Evaluator{
		client: client,
	}
}

func (e *Evaluator) EvalExpr(ctx context.Context, ast parser.SQLQuery) ([]object.Row, error) {
	return e.evalSelect(ctx, ast.Select)
}

func (e *Evaluator) evalSelect(ctx context.Context, sel parser.Select) ([]object.Row, error) {
	from, err := e.evalFrom(ctx, sel.From)
	if err != nil {
		return nil, fmt.Errorf("eval from: %w", err)
	}

	var all bool
	for _, s := range sel.Fields {
		if s.Column == "*" {
			all = true
		}
	}

	if all {
		return from, nil
	}

	out := make([]object.Row, 0, len(from))
	for _, row := range from {
		var ins object.Row
		for _, f := range sel.Fields {
			ins[f.Column] = row[f.Column]
		}
	}
	return out, nil
}

func (e *Evaluator) evalFrom(ctx context.Context, from parser.From) ([]object.Row, error) {
	if from.Where != nil && from.Where.Column == "id" {
		r, ok, err := e.client.Get(ctx, from.Table, from.Where.Column)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, nil
		}
		return []object.Row{r}, nil
	}

	rows, err := e.client.Scan(ctx, from.Table)
	if err != nil {
		return nil, err
	}

	if from.Where != nil {
		var out []object.Row
		for _, r := range rows {
			out = append(out, r)
		}
		return out, nil
	}

	return rows, nil
}
