package btree

import (
	"fmt"
	"strings"
	"testing"
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
			b := NewBTree[int](tc.order)

			for _, a := range tc.given {
				b.Add(a, []byte(fmt.Sprint(a)))
				fmt.Println(pretty(b.root))
			}
		})
	}
}

type treeFmt struct {
	key      int
	value    string
	children []treeFmt
}

func pretty(n *node[int]) string {
	if n.leaf() {
		out := make([]string, 0, len(n.keys))
		for _, k := range n.keys {
			out = append(out, fmt.Sprintf("%d=%s", k.key, k.val))
		}
		return strings.Join(out, " ")
	}

	refs := make([]string, 0, len(n.refs))
	for _, r := range n.refs {
		s, _ := r.n.Get()
		refs = append(refs, pretty(s))
	}

	return strings.Join(refs, " -- ")
}

func ptr[T any](t T) *T {
	return &t
}

func Test_insertRefs(t *testing.T) {
	nodeA := node[int]{}
	nodeB := node[int]{}
	nodeC := node[int]{}

	tests := map[string]struct {
		given  []*ref[int]
		adding []*ref[int]
		want   []*ref[int]
	}{
		"with bounds": {
			given: []*ref[int]{
				{
					from: ptr(10),
					to:   ptr(20),
					n:    newInMemory[int](&nodeA),
				},
			},
			adding: []*ref[int]{
				{
					from: nil,
					to:   ptr(15),
					n:    newInMemory[int](&nodeB),
				},
				{
					from: ptr(15),
					to:   nil,
					n:    newInMemory[int](&nodeC),
				},
			},
			want: []*ref[int]{
				{
					from: ptr(10),
					to:   ptr(15),
					n:    newInMemory[int](&nodeA),
				},
				{
					from: ptr(15),
					to:   ptr(20),
					n:    newInMemory[int](&nodeB),
				},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := insertRefs[int](tc.given, tc.adding)

			for i := range got {
				if *got[i].from != *tc.want[i].from {
					t.Errorf("index %d: got from=%d, want %d", i, *got[i].from, *tc.want[i].from)
				}
				if *got[i].to != *tc.want[i].to {
					t.Errorf("index %d: got to=%d, want %d", i, *got[i].to, *tc.want[i].to)
				}
			}
		})
	}
}
