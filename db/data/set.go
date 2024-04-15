package data

import (
	"context"
	"fmt"
)

func (d *DB) Set(ctx context.Context, table string, id string, data []byte) error {
	err := d.s.Add(ctx, id, data)
	if err != nil {
		return fmt.Errorf("save data: %w", err)
	}

	return nil
}
