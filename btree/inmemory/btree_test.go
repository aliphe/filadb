package inmemory

import (
	"context"
	"strconv"
	"testing"

	"github.com/aliphe/filadb/btree"
)

func Test_Btree(t *testing.T) {
	tests := map[string]struct {
		given []int
		order int
	}{
		"order 3": {
			order: 3,
			given: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			b := btree.New(New[int]())

			for _, a := range tc.given {
				b.Add(context.Background(), "root", a, []byte(strconv.Itoa(a)))
			}
			out, err := b.Print("root")
			if err != nil {
				t.Fatal(err)
			}
			t.Log("\n" + out)
		})
	}
}
