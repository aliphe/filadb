package btree

import (
	"cmp"
	"fmt"
	"slices"
)

type k interface {
	cmp.Ordered
}

type nodeAccessor[K k] interface {
	Get() (*node[K], error)
}

type keyVal[K k] struct {
	key K
	val []byte
}

type ref[K k] struct {
	from *K
	to   *K
	n    nodeAccessor[K]
}

func (n *node[K]) leaf() bool {
	return len(n.keys) > 0
}

type node[K k] struct {
	keys []*keyVal[K]
	refs []*ref[K]
}

type BTree[K k] struct {
	order int
	root  *node[K]
}

func NewBTree[K k](order int) *BTree[K] {
	return &BTree[K]{
		order: order,
		root:  nil,
	}
}

func leaf[K k](kv []*keyVal[K]) *node[K] {
	return &node[K]{
		keys: kv,
		refs: nil,
	}
}

func nonLeaf[K k](refs []*ref[K]) *node[K] {
	return &node[K]{
		keys: nil,
		refs: refs,
	}
}

func (b *BTree[K]) Add(key K, val []byte) error {
	if b.root == nil {
		b.root = leaf([]*keyVal[K]{{
			key: key,
			val: val,
		}})
		return nil
	}

	newRoot, err := b.insert(b.root, &keyVal[K]{
		key: key,
		val: val,
	})
	if err != nil {
		return fmt.Errorf("insert key: %w", err)
	}

	if newRoot != nil {
		b.root = nonLeaf[K](newRoot)
	}

	return nil
}

func findNode[K k](refs []*ref[K], k K) (*node[K], error) {
	var ref *ref[K]
	for _, r := range refs {
		if r.to == nil || *r.to > k {
			ref = r
		}
	}

	return ref.n.Get()
}

func (b *BTree[K]) insert(n *node[K], kv *keyVal[K]) ([]*ref[K], error) {
	var movingUp []*ref[K]
	if !n.leaf() {
		r, err := findNode[K](n.refs, kv.key)
		if err != nil {
			return nil, fmt.Errorf("find node to insert value: %w", err)
		}

		movingUp, err = b.insert(r, kv)
		if err != nil {
			// we don't know how many recursion we had, don't want to wrap too much
			return nil, err
		}
	} else {
		n.keys = append(n.keys, kv)
		slices.SortFunc(n.keys, func(a, b *keyVal[K]) int {
			return cmp.Compare(a.key, b.key)
		})
	}

	if movingUp != nil {
		n.refs = insertRefs[K](n.refs, movingUp)
	}

	if len(n.keys) > b.order {
		mid := (b.order + 1) / 2
		leaves := []*node[K]{
			leaf[K](n.keys[:mid]),
			leaf[K](n.keys[mid:]),
		}

		return []*ref[K]{
			{
				from: nil,
				to:   &n.keys[mid].key,
				n:    newInMemory(leaves[0]),
			},
			{
				from: &n.keys[mid].key,
				to:   nil,
				n:    newInMemory(leaves[1]),
			},
		}, nil
	}

	if len(n.refs) > b.order {
		mid := (b.order + 1) / 2
		nonLeaves := []*node[K]{
			nonLeaf[K](n.refs[:mid]),
			nonLeaf[K](n.refs[mid:]),
		}

		return []*ref[K]{
			{
				from: nil,
				to:   n.refs[mid].from,
				n:    newInMemory(nonLeaves[0]),
			},
			{
				from: n.refs[mid].from,
				to:   nil,
				n:    newInMemory(nonLeaves[1]),
			},
		}, nil
	}

	return nil, nil
}

type inMemoryNodeAccessor[K k] struct {
	node *node[K]
}

func newInMemory[K k](node *node[K]) *inMemoryNodeAccessor[K] {
	return &inMemoryNodeAccessor[K]{
		node,
	}
}

func (i *inMemoryNodeAccessor[K]) Get() (*node[K], error) {
	return i.node, nil
}

func insertRefs[K k](refs []*ref[K], new []*ref[K]) []*ref[K] {
	var merged []*ref[K]

	for _, curr := range refs {
		if (curr.from == nil || *new[0].to > *curr.from) &&
			(curr.to == nil || *new[0].to < *curr.to) {

			merged = append(merged, &ref[K]{
				from: curr.from,
				to:   new[0].to,
				n:    new[0].n,
			}, &ref[K]{
				from: new[0].to, // or new[1].from
				to:   curr.to,
				n:    new[1].n,
			})
		} else {
			merged = append(merged, curr)
		}
	}
	return merged
}
