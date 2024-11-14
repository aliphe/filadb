package csv

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"

	"github.com/aliphe/filadb/db/object"
)

type Writer struct {
	w io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{
		w: w,
	}
}

func (w *Writer) Write(rows []object.Row) error {
	if len(rows) == 0 {
		return errors.New("empty rows slice")
	}
	records := make([][]string, 0, len(rows)+1)
	cols := make([]string, 0, len(rows[0]))
	for k := range rows[0] {
		cols = append(cols, k)
	}
	records = append(records, cols)

	for _, r := range rows {
		vals := make([]string, 0, len(rows[0]))
		for _, k := range cols {
			vals = append(vals, fmt.Sprint(r[k]))
		}
		records = append(records, vals)
	}

	writer := csv.NewWriter(w.w)
	writer.WriteAll(records)
	writer.Flush()
	return writer.Error()
}
