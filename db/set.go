package db

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
)

type Row interface{}

func (d *DB) Set(ctx context.Context, data Row) error {
	var b []byte

	buf := bytes.NewBuffer(b)

	enc := json.NewEncoder(buf)
	err := enc.Encode(data)
	if err != nil {
		return fmt.Errorf("encode data: %w", err)
	}

	_, err = d.f.Write(buf.Bytes())
	if err != nil {
		return fmt.Errorf("write data: %w", err)
	}

	return nil
}
