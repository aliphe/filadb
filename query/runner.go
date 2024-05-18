package query

import (
	"context"

	"github.com/aliphe/filadb/db/object"
)

type Runner interface {
	Run(context.Context, string) ([]object.Row, error)
}
