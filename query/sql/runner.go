package sql

import (
	"context"
	"fmt"

	"github.com/aliphe/filadb/db"
	"github.com/aliphe/filadb/db/object"
	"github.com/aliphe/filadb/query/sql/eval"
	"github.com/aliphe/filadb/query/sql/lexer"
	"github.com/aliphe/filadb/query/sql/parser"
)

type Runner struct {
	db *db.Client
}

func NewRunner(db *db.Client) *Runner {
	return &Runner{
		db: db,
	}
}

func (r *Runner) Run(ctx context.Context, expr string) ([]object.Row, error) {
	tokens, err := lexer.Tokenize(expr)
	if err != nil {
		return nil, err
	}
	ast, err := parser.Parse(tokens)
	if err != nil {
		return nil, fmt.Errorf("parsing expression: %w", err)
	}

	eval := eval.New(r.db)
	out, err := eval.EvalExpr(ctx, ast)
	if err != nil {
		return nil, fmt.Errorf("eval expression: %w", err)
	}

	return out, nil
}
