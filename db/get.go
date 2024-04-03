package db

import (
	"context"
	"encoding/json"
	"fmt"
)

var readBlockSize int64 = 4096

func (d *DB) Get(ctx context.Context, table, id string) (any, bool, error) {
	stat, err := d.f.Stat()
	if err != nil {
		return nil, false, fmt.Errorf("stat file %s: %w", d.f.Name(), err)
	}

	s := stat.Size()

	var rest []byte

	var off = s
	for off > 0 {
		off = max(off-readBlockSize, 0)
		rs := min(readBlockSize, s-off)
		buf := make([]byte, rs)

		_, err := d.f.ReadAt(buf, off)
		if err != nil {
			return nil, false, fmt.Errorf("read file %s at offset %d: %w", d.f.Name(), s, err)
		}

		lines, r := split(append(rest, buf...), '\n')
		rest = r

		row, found, err := seek(lines, id)
		if err != nil {
			return nil, false, fmt.Errorf("page read: %w", err)
		}
		if found {
			return string(row), found, nil
		}
	}

	return nil, false, nil
}

type WithID struct {
	ID string `json:"id"`
}

func seek(lines [][]byte, id string) ([]byte, bool, error) {
	var r WithID
	for i := len(lines) - 1; i >= 0; i-- {
		if err := json.Unmarshal(lines[i], &r); err != nil {
			return nil, false, fmt.Errorf("unmarshal row: %w", err)
		}

		if r.ID == id {
			return lines[i], true, nil
		}
	}

	return nil, false, nil
}

func split(b []byte, delimiter rune) ([][]byte, []byte) {
	var l [][]byte

	var cur []byte
	for _, c := range b {
		if rune(c) == delimiter {
			l = append(l, cur)
			cur = nil
			continue
		}
		cur = append(cur, c)
	}
	return l, cur
}

func max(a, b int64) int64 {
	if a < b {
		return b
	}
	return a
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
