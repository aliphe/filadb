package table

import (
	"context"
	"fmt"
)

func (d *Store) Set(ctx context.Context, table, row string, data []byte) error {
	err := d.s.Add(ctx, table, row, data)
	if err != nil {
		return fmt.Errorf("save data: %w", err)
	}

	return nil
}
