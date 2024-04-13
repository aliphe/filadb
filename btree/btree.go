package btree

import (
	"cmp"
	"context"
	"fmt"
	"slices"
)

type Key interface {
	cmp.Ordered
}

type KeyVal[K Key] struct {
	Key K
	Val []byte
}

type NodeID string

type Ref[K Key] struct {
	From *K
	To   *K
	N    NodeID
}

type nodeStore[K Key] interface {
	Leaf(context.Context, []*KeyVal[K]) (*Node[K], error)
	NonLeaf(context.Context, []*Ref[K]) (*Node[K], error)
	Find(context.Context, NodeID) (*Node[K], bool, error)
}

type BTree[K Key] struct {
	order int
	root  *Node[K]
	store nodeStore[K]
}

func New[K Key](order int, store nodeStore[K]) *BTree[K] {
	return &BTree[K]{
		order: order,
		root:  nil,
		store: store,
	}
}

func (b *BTree[K]) Add(ctx context.Context, key K, val []byte) error {
	if b.root == nil {
		root, err := b.store.Leaf(ctx, []*KeyVal[K]{{
			Key: key,
			Val: val,
		}})
		if err != nil {
			return fmt.Errorf("create root node: %w", err)
		}
		b.root = root
		return nil
	}

	newRoot, err := b.insert(ctx, b.root, &KeyVal[K]{
		Key: key,
		Val: val,
	})
	if err != nil {
		return fmt.Errorf("insert key: %w", err)
	}

	if newRoot != nil {
		root, err := b.store.NonLeaf(ctx, newRoot)
		if err != nil {
			return fmt.Errorf("create new root node: %w", err)
		}
		b.root = root
	}

	return nil
}

func (b *BTree[K]) findNode(ctx context.Context, refs []*Ref[K], k K) (*Node[K], error) {
	var ref *Ref[K]
	for _, r := range refs {
		if r.To == nil || *r.To > k {
			ref = r
		}
	}

	node, ok, err := b.store.Find(ctx, ref.N)
	if err != nil {
		return nil, fmt.Errorf("following node ref: %w", err)
	}
	if !ok {
		return nil, fmt.Errorf("%w: %w", ErrTreeCorrupted, err)
	}

	return node, nil
}

func (b *BTree[K]) insert(ctx context.Context, n *Node[K], kv *KeyVal[K]) ([]*Ref[K], error) {
	var keys []*KeyVal[K]
	var refs []*Ref[K]

	var movingUp []*Ref[K]
	if !n.Leaf() {
		r, err := b.findNode(ctx, n.Refs(), kv.Key)
		if err != nil {
			return nil, fmt.Errorf("find node to insert value: %w", err)
		}

		movingUp, err = b.insert(ctx, r, kv)
		if err != nil {
			return nil, err
		}
	} else {
		keys = append(n.Keys(), kv)
		slices.SortFunc(keys, func(a, b *KeyVal[K]) int {
			return cmp.Compare(a.Key, b.Key)
		})
	}

	if movingUp != nil {
		refs = insertRefs[K](n.Refs(), movingUp)
	}

	if len(keys) > b.order {
		mid := (b.order + 1) / 2
		left, err := b.store.Leaf(ctx, keys[:mid])
		if err != nil {
			return nil, fmt.Errorf("split node: %w", err)
		}
		right, err := b.store.Leaf(ctx, keys[mid:])
		if err != nil {
			return nil, fmt.Errorf("split node: %w", err)
		}

		return []*Ref[K]{
			{
				From: nil,
				To:   &keys[mid].Key,
				N:    left.id,
			},
			{
				From: &keys[mid].Key,
				To:   nil,
				N:    right.id,
			},
		}, nil
	}

	if len(refs) > b.order {
		mid := (b.order + 1) / 2
		left, err := b.store.NonLeaf(ctx, refs[:mid])
		if err != nil {
			return nil, fmt.Errorf("split node: %w", err)
		}
		right, err := b.store.NonLeaf(ctx, refs[mid:])
		if err != nil {
			return nil, fmt.Errorf("split node: %w", err)
		}

		return []*Ref[K]{
			{
				From: nil,
				To:   refs[mid].From,
				N:    left.id,
			},
			{
				From: refs[mid].To,
				To:   nil,
				N:    right.id,
			},
		}, nil
	}

	return nil, nil
}

func insertRefs[K Key](refs []*Ref[K], new []*Ref[K]) []*Ref[K] {
	var merged []*Ref[K]

	for _, curr := range refs {
		if (curr.From == nil || *new[0].To > *curr.From) &&
			(curr.To == nil || *new[0].To < *curr.To) {

			merged = append(merged, &Ref[K]{
				From: curr.From,
				To:   new[0].To,
				N:    new[0].N,
			}, &Ref[K]{
				From: new[0].To, // or new[1].from
				To:   curr.To,
				N:    new[1].N,
			})
		} else {
			merged = append(merged, curr)
		}
	}
	return merged
}
