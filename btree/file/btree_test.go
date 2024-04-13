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
			given: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
		},
	}

	err := os.Mkdir(".db", os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(".db")

	f, err := os.Open(".db")
	if err != nil {
		t.Fatal(err)
	}
	b, err := New[int](f)
	if err != nil {
		t.Fatal(err)
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			b := btree.New(tc.order, b)

			for _, a := range tc.given {
				b.Add(context.Background(), a, []byte(strconv.Itoa(a)))
				out, err := b.Print()
				if err != nil {
					t.Fatal(err)
				}
				t.Log("\n" + out)
			}
		})
	}
}
