package file

import (
	"context"
	"os"
	"strconv"
	"testing"

	"github.com/aliphe/filadb/btree"
	"github.com/google/go-cmp/cmp"
)

func Test_Btree(t *testing.T) {
	tests := map[string]struct {
		given []int
		want  string
		order int
	}{
		"single layer": {
			order: 30,
			given: []int{1, 2, 3, 4, 5, 6},
			want:  "1,2,3,4,5,6",
		},
		"two layers": {
			order: 3,
			given: []int{1, 2, 3, 4, 5, 6},
			want:  "]-∞;3[(1,2)[3;5[(3,4)[5;∞[(5,6)",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			b, err := New[int](WithPath(t.TempDir()))
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			defer os.RemoveAll(t.TempDir())
			bt := btree.New(b, btree.WithOrder(tc.order))
			ctx := context.Background()

			for _, a := range tc.given {
				bt.Add(ctx, "root", a, []byte(strconv.Itoa(a)))
			}
			out, err := bt.Print(ctx, "root")
			if err != nil {
				t.Fatal(err)
			}
			t.Log(out)
			if diff := cmp.Diff(tc.want, out); diff != "" {
				t.Fatalf("Print() mismatch (-want,+got): %s", diff)
			}
		})
	}
}
