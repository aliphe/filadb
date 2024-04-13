package btree

import (
	"testing"
)

type treeFmt struct {
	key      int
	value    string
	children []treeFmt
}

func ptr[T any](t T) *T {
	return &t
}

func Test_insertRefs(t *testing.T) {
	tests := map[string]struct {
		given  []*Ref[int]
		adding []*Ref[int]
		want   []*Ref[int]
	}{
		"with bounds": {
			given: []*Ref[int]{
				{
					From: ptr(10),
					To:   ptr(20),
					N:    "12",
				},
			},
			adding: []*Ref[int]{
				{
					From: nil,
					To:   ptr(15),
					N:    "13",
				},
				{
					From: ptr(15),
					To:   nil,
					N:    "13",
				},
			},
			want: []*Ref[int]{
				{
					From: ptr(10),
					To:   ptr(15),
					N:    "12",
				},
				{
					From: ptr(15),
					To:   ptr(20),
					N:    "13",
				},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := insertRefs(tc.given, tc.adding)

			for i := range got {
				if *got[i].From != *tc.want[i].From {
					t.Errorf("index %d: got from=%d, want %d", i, *got[i].From, *tc.want[i].From)
				}
				if *got[i].To != *tc.want[i].To {
					t.Errorf("index %d: got to=%d, want %d", i, *got[i].To, *tc.want[i].To)
				}
			}
		})
	}
}
