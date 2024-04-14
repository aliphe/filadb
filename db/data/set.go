package data

import (
	"context"
	"encoding/json"
	"fmt"
)

func (d *DB) Set(ctx context.Context, table string, id string, data any) error {
	enc, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshal data: %w", err)
	}

	err = d.s.Add(ctx, id, enc)
	if err != nil {
		return fmt.Errorf("save data: %w", err)
	}

	return nil
}
