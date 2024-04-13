package inmemory

import (
	"context"

	"github.com/aliphe/filadb/btree"
)

type btreeStore[K btree.Key] struct {
	db map[btree.NodeID]*btree.Node[K]
}

func New[K btree.Key]() *btreeStore[K] {
	return &btreeStore[K]{
		db: make(map[btree.NodeID]*btree.Node[K]),
	}
}

func groupKv[K btree.Key](kv []*btree.KeyVal[K]) map[K]*btree.KeyVal[K] {
	out := make(map[K]*btree.KeyVal[K], len(kv))
	for _, k := range kv {
		out[k.Key] = k
	}

	return out
}

func (s *btreeStore[K]) Save(ctx context.Context, n *btree.Node[K]) error {
	s.db[n.ID()] = n
	return nil

}

func (s *btreeStore[K]) Find(ctx context.Context, id btree.NodeID) (*btree.Node[K], bool, error) {
	n, ok := s.db[id]
	return n, ok, nil
}
