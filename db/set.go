package db

import (
	"context"
	"encoding/json"
	"fmt"
)

const separator byte = '\n'

func (d *DB) Set(ctx context.Context, table string, data any) error {
	enc, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshal data: %w", err)
	}

	f, err := fileWriter(table)
	if err != nil {
		return fmt.Errorf("access storage: %w", err)
	}
	_, err = f.Write(append(enc, separator))
	if err != nil {
		return fmt.Errorf("write data: %w", err)
	}

	return nil
}
