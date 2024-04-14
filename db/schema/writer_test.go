package schema

import (
	"context"
	"testing"

	"github.com/aliphe/filadb/btree"
	"github.com/aliphe/filadb/btree/inmemory"
)

func Test_Writer(t *testing.T) {
	s := inmemory.New[string]()
	btree := btree.New(100, s)

	w, err := NewWriter(btree)
	if err != nil {
		t.Fatal(err)
	}

	err = w.Create(context.Background(), &Schema{
		Table: "user",
		Properties: []*Property{
			{
				Name:       "id",
				Type:       PropertyTypeText,
				PrimaryKey: true,
			},
			{
				Name:       "name",
				Type:       PropertyTypeText,
				PrimaryKey: false,
			},
		},
	})

	if err != nil {
		t.Fatal(err)
	}
}
