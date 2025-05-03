package sql

import (
	"context"
	"fmt"

	"github.com/aliphe/filadb/db"
	"github.com/aliphe/filadb/query/sql/eval"
	"github.com/aliphe/filadb/query/sql/lexer"
	"github.com/aliphe/filadb/query/sql/parser"
	"github.com/aliphe/filadb/query/sql/validation"
)

type Runner struct {
	db *db.Client
}

func NewRunner(db *db.Client) *Runner {
	return &Runner{
		db: db,
	}
}

func (r *Runner) Run(ctx context.Context, expr string) ([]byte, error) {
	tokens, err := lexer.Tokenize(expr)
	if err != nil {
		return nil, err
	}

	q, err := parser.Parse(tokens)
	if err != nil {
		return nil, fmt.Errorf("parsing expression: %w", err)
	}

	shape, err := r.db.Shape(ctx)
	if err != nil {
		return nil, err
	}

	sc := validation.NewSanityChecker(shape)

	if err := sc.Check(q); err != nil {
		return nil, err
	}

	eval := eval.New(r.db)
	out, err := eval.EvalExpr(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("eval expression: %w", err)
	}

	return out, nil
}
