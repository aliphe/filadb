package table

import (
	"context"
)

func (d *Store) Get(ctx context.Context, table, row string) ([]byte, bool, error) {
	return d.s.Get(ctx, table, row)
}
