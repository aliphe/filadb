package inmemory

import (
	"context"
	"strconv"

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

func (s *btreeStore[K]) Leaf(ctx context.Context, kv []*btree.KeyVal[K]) (*btree.Node[K], error) {
	id := btree.NodeID(strconv.Itoa(len(s.db)))
	n := btree.NewNode[K](id, kv, nil)
	s.db[id] = n
	return n, nil

}

func (s *btreeStore[K]) NonLeaf(ctx context.Context, refs []*btree.Ref[K]) (*btree.Node[K], error) {
	id := btree.NodeID(strconv.Itoa(len(s.db)))
	n := btree.NewNode[K](id, nil, refs)
	s.db[id] = n
	return n, nil
}

func (s *btreeStore[K]) Find(ctx context.Context, id btree.NodeID) (*btree.Node[K], bool, error) {
	n, ok := s.db[id]
	return n, ok, nil
}
