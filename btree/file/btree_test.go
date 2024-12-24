package file

import (
	"context"
	"os"
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
			given: []int{5, 4, 3, 2, 1, 0, 2, 4, 2, 1},
		},
	}

	b, err := New[int](WithPath(".testdb"))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	defer os.RemoveAll(".testdb")

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			b := btree.New(b, btree.WithOrder(tc.order))

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
