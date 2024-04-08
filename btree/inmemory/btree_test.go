package inmemory

import (
	"context"
	"fmt"
	"testing"

	"github.com/aliphe/filadb/btree"
)

func Test_Btree(t *testing.T) {
	tests := map[string]struct {
		given []int
		order int
	}{
		"order 5": {
			order: 5,
			given: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			b := btree.New[int](tc.order, New[int]())

			for _, a := range tc.given {
				b.Add(context.Background(), a, []byte(fmt.Sprint(a)))
			}
		})
	}
}
