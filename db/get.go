package db

import (
	"context"
)

var readBlockSize int64 = 4096

func (d *DB) Get(ctx context.Context, table, id string) ([]byte, bool, error) {
	return d.s.Get(ctx, id)
}
