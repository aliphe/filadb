package btree

type Node[K Key] struct {
	id   NodeID
	keys map[K]*KeyVal[K]
	refs []*Ref[K]
}

func (n *Node[K]) ID() NodeID {
	return n.ID()
}

func NewNode[K Key](id NodeID, keys []*KeyVal[K], refs []*Ref[K]) *Node[K] {
	km := make(map[K]*KeyVal[K], len(keys))
	for _, k := range keys {
		km[k.Key] = k
	}
	return &Node[K]{
		id:   id,
		keys: km,
		refs: refs,
	}
}

func (n *Node[K]) Leaf() bool {
	return len(n.keys) > 0
}

func (n *Node[K]) Value(key K) (*KeyVal[K], bool) {
	v, ok := n.keys[key]
	return v, ok
}

func (n *Node[K]) Refs() []*Ref[K] {
	refs := make([]*Ref[K], 0, len(n.refs))
	for _, r := range n.refs {
		refs = append(refs, r)
	}

	return refs
}

func (n *Node[K]) Keys() []*KeyVal[K] {
	keys := make([]*KeyVal[K], 0, len(n.keys))
	for _, k := range n.keys {
		keys = append(keys, k)
	}

	return keys
}
