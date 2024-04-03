package db

import (
	"context"
	"fmt"
	"os"
	"testing"
)

func Test_Set(t *testing.T) {
	type row struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}

	tests := []struct {
		ins row
	}{
		{
			ins: row{
				ID:   "1",
				Name: "tust",
			},
		},
	}

	f, err := os.OpenFile("db.txt", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Errorf("open db file: %v", err.Error())
	}
	db := New(f)

	for _, tc := range tests {
		err := db.Set(context.Background(), tc.ins)
		if err != nil {
			t.Fatal(err)
		}
		r, f, err := db.Get(context.Background(), "table", tc.ins.ID)
		if err != nil {
			t.Fatal(err)
		}
		if f != true {
			t.Fatal("inserted row now found")
		}
		t.Logf("row: %v", r)
	}
}

func Test_split(t *testing.T) {
	tests := []struct {
		in   []byte
		out  [][]byte
		rest []byte
	}{
		{
			in:   []byte("test"),
			out:  nil,
			rest: []byte("test"),
		},
		{
			in:   []byte("test\ntust\n"),
			out:  [][]byte{[]byte("test"), []byte("tust")},
			rest: nil,
		},
	}

	for idx, tc := range tests {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			o, rest := split(tc.in, '\n')

			if len(o) != len(tc.out) {
				t.Errorf("lines: want %d, got %d", len(tc.out), len(o))
			}
			if string(rest) != string(tc.rest) {
				t.Errorf("rest: want %s, got %s", string(tc.rest), string(rest))
			}
			for i := 0; i < len(o); i++ {
				if string(o[i]) != string(tc.out[i]) {
					t.Errorf("rest: want %s, got %s", string(tc.out[i]), string(o[i]))
				}
			}
		})
	}
}
