package db

import (
	"context"
)

func (d *DB) Get(ctx context.Context, table, id string) ([]byte, bool, error) {
	return d.s.Get(ctx, id)
}
