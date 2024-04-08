package inmemory

import "github.com/aliphe/filadb/btree"

type node[K btree.Key] struct {
	id   btree.NodeID
	keys map[K]*btree.KeyVal[K]
	refs []*btree.Ref[K]
}

func (n *node[K]) ID() btree.NodeID {
	return n.id
}

func (n *node[K]) Leaf() bool {
	return len(n.keys) > 0
}

func (n *node[K]) Value(key K) (*btree.KeyVal[K], bool) {
	v, ok := n.keys[key]
	return v, ok
}

func (n *node[K]) Refs() []*btree.Ref[K] {
	refs := make([]*btree.Ref[K], 0, len(n.refs))
	for _, r := range n.refs {
		refs = append(refs, r)
	}

	return refs
}

func (n *node[K]) Keys() []*btree.KeyVal[K] {
	keys := make([]*btree.KeyVal[K], 0, len(n.keys))
	for _, k := range n.keys {
		keys = append(keys, k)
	}

	return keys
}
