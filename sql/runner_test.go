package sql

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/aliphe/filadb/btree"
	"github.com/aliphe/filadb/btree/inmemory"
	"github.com/aliphe/filadb/db"
)

func Test_Run(t *testing.T) {
	tests := map[string]struct {
		want io.Reader
	}{
		"SELECT * FROM users WHERE id = 1;": {
			want: nil,
		},
	}

	store := inmemory.New[string]()
	btree := btree.New(100, store)

	db := db.NewClient(btree)

	runner := NewRunner(db)

	for expr := range tests {
		t.Run(expr, func(t *testing.T) {
			got, err := runner.Run(context.Background(), expr)
			if err != nil {
				t.Logf("unexpected error: %s", err)
			}

			fmt.Printf("%+v", got)
		})
	}
}
