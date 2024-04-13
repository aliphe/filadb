package btree

import "github.com/google/uuid"

type Node[K Key] struct {
	id   NodeID
	keys []*KeyVal[K]
	refs []*Ref[K]
}

func newNodeID() NodeID {
	return NodeID(uuid.New().String())
}

func leaf[K Key](keys []*KeyVal[K]) *Node[K] {
	id := newNodeID()
	return &Node[K]{
		id:   id,
		keys: keys,
		refs: nil,
	}
}

func nonLeaf[K Key](refs []*Ref[K]) *Node[K] {
	id := newNodeID()
	return &Node[K]{
		id:   id,
		keys: nil,
		refs: refs,
	}
}

func (n *Node[K]) ID() NodeID {
	return n.id
}

func (n *Node[K]) Leaf() bool {
	return len(n.keys) > 0
}

func (n *Node[K]) Value(key K) (*KeyVal[K], bool) {
	for _, i := range n.keys {
		if i.Key == key {
			return i, true
		}
	}
	return nil, false
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

func (n *Node[K]) SetKeys(keys []*KeyVal[K]) {
	n.keys = keys
}

func (n *Node[K]) SetRefs(refs []*Ref[K]) {
	n.refs = refs
}
