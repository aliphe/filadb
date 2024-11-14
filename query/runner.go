package query

import (
	"context"
)

type Runner interface {
	Run(context.Context, string) ([]byte, error)
}
