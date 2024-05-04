package sql

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/aliphe/filadb/db"
	"github.com/aliphe/filadb/sql/lexer"
	"github.com/aliphe/filadb/sql/parser"
)

type Runner struct {
	client *db.Client
}

func NewRunner(client *db.Client) *Runner {
	return &Runner{
		client: client,
	}
}

func (r *Runner) Run(ctx context.Context, expr string) (io.Reader, error) {
	tokens, err := lexer.Tokenize(expr)
	if err != nil {
		return nil, err
	}
	_, err = parser.Parse(tokens)
	if err != nil {
		return nil, fmt.Errorf("parsing expression: %w", err)
	}

	return strings.NewReader("done"), nil
}
