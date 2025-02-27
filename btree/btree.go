package btree

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	"github.com/aliphe/filadb/db/storage"
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
	Save(context.Context, *Node[K]) error
	Find(context.Context, NodeID) (*Node[K], bool, error)
}

type BTree[K Key] struct {
	order  int
	store  nodeStore[K]
	rootID NodeID
}

type options struct {
	order int
}

type Option func(*options)

func WithOrder(order int) Option {
	return func(o *options) {
		o.order = order
	}
}

func New[K Key](store nodeStore[K], opts ...Option) *BTree[K] {
	opt := options{
		order: 500,
	}

	for _, o := range opts {
		o(&opt)
	}

	btree := BTree[K]{
		order: opt.order,
		store: store,
	}

	return &btree
}

func (b *BTree[K]) createRoot(ctx context.Context, nodeID NodeID, key K, val []byte) error {
	root := &Node[K]{
		id: nodeID,
		keys: []*KeyVal[K]{
			{
				Key: key,
				Val: val,
			},
		},
		refs: nil,
	}

	if err := b.store.Save(ctx, root); err != nil {
		return fmt.Errorf("save root node: %w", err)
	}

	return nil
}

func (b *BTree[K]) root(ctx context.Context, nodeID NodeID) (*Node[K], bool, error) {
	root, ok, err := b.store.Find(ctx, nodeID)
	if err != nil {
		return nil, false, fmt.Errorf("acquire btree root: %w", err)
	}
	if !ok {
		return nil, false, nil
	}
	return root, true, nil
}

func (b *BTree[K]) updateRoot(ctx context.Context, curr *Node[K], refs []*Ref[K]) error {
	rootID := curr.ID()
	curr.id = newNodeID()
	if err := b.store.Save(ctx, curr); err != nil {
		return fmt.Errorf("unmap old root: %w", err)
	}

	root := &Node[K]{
		id:   rootID,
		keys: nil,
		refs: refs,
	}

	return b.store.Save(ctx, root)
}

func (b *BTree[K]) Add(ctx context.Context, node string, key K, val []byte) error {
	return b.set(ctx, node, key, val, false)
}

func (b *BTree[K]) Set(ctx context.Context, node string, key K, val []byte) error {
	return b.set(ctx, node, key, val, true)
}

func (b *BTree[K]) set(ctx context.Context, node string, key K, val []byte, update bool) error {
	root, ok, err := b.root(ctx, NodeID(node))
	if err != nil {
		return fmt.Errorf("acquire root: %w", err)
	}

	if !ok {
		return b.createRoot(ctx, NodeID(node), key, val)
	}

	newRoot, err := b.insert(ctx, root, &KeyVal[K]{
		Key: key,
		Val: val,
	}, update)
	if err != nil {
		return err
	}

	if newRoot != nil {
		return b.updateRoot(ctx, root, newRoot)
	}

	return nil

}

func (b *BTree[K]) Get(ctx context.Context, node string, key K) ([][]byte, error) {
	root, ok, err := b.root(ctx, NodeID(node))
	if err != nil {
		return nil, fmt.Errorf("acquire root: %w", err)
	}
	if !ok {
		return nil, storage.ErrTableNotFound
	}

	got, err := b.get(ctx, root, key)
	if err != nil {
		return nil, err
	}
	return got, nil
}

func (b *BTree[K]) Scan(ctx context.Context, node string) ([][]byte, error) {
	root, ok, err := b.root(ctx, NodeID(node))
	if err != nil {
		return nil, fmt.Errorf("acquire root: %w", err)
	}
	if !ok {
		return nil, storage.ErrTableNotFound
	}

	return b.dump(ctx, root)
}

func (b *BTree[K]) dump(ctx context.Context, n *Node[K]) ([][]byte, error) {
	if !n.Leaf() {
		out := make([][][]byte, 0, b.order)
		// TODO parallel (needs benchmark)
		for _, r := range n.Refs() {
			c, ok, err := b.store.Find(ctx, r.N)
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, storage.ErrTableNotFound
			}

			b, err := b.dump(ctx, c)
			if err != nil {
				return nil, err
			}

			out = append(out, b)
		}
		return slices.Concat(out...), nil
	}

	out := make([][]byte, 0, b.order)
	for _, kv := range n.keys {
		out = append(out, kv.Val)
	}
	return out, nil
}

func (b *BTree[K]) get(ctx context.Context, n *Node[K], k K) ([][]byte, error) {
	if n.Leaf() {
		var found [][]byte
		for _, kv := range n.Keys() {
			if kv.Key == k {
				found = append(found, kv.Val)
			}
		}
		return found, nil
	}

	sub, err := b.findInNode(ctx, n, k)
	if err != nil {
		return nil, err
	}

	return b.get(ctx, sub, k)
}

func (b *BTree[K]) findInNode(ctx context.Context, n *Node[K], k K) (*Node[K], error) {
	var ref *Ref[K]
	for _, r := range n.Refs() {
		if r.To == nil || *r.To > k {
			ref = r
			break
		}
	}

	node, ok, err := b.store.Find(ctx, ref.N)
	if err != nil {
		return nil, fmt.Errorf("following node ref: %w", err)
	}
	if !ok {
		return nil, fmt.Errorf("%w: %w", storage.ErrTableNotFound, err)
	}

	return node, nil
}

func (b *BTree[K]) insert(ctx context.Context, n *Node[K], kv *KeyVal[K], update bool) ([]*Ref[K], error) {
	var keys []*KeyVal[K] = n.Keys()
	var refs []*Ref[K] = n.Refs()

	var movingUp []*Ref[K]
	if !n.Leaf() {
		r, err := b.findInNode(ctx, n, kv.Key)
		if err != nil {
			return nil, fmt.Errorf("find node to insert value: %w", err)
		}

		movingUp, err = b.insert(ctx, r, kv, update)
		if err != nil {
			return nil, err
		}
		if movingUp != nil {
			refs = insertRefs(n.Refs(), movingUp)
			movingUp = nil
		}
	} else {
		if update {
			for i := range n.keys {
				if n.keys[i].Key == kv.Key {
					n.keys[i].Val = kv.Val
				}
			}
		} else {
			keys = append(n.Keys(), kv)
		}
		slices.SortFunc(keys, func(a, b *KeyVal[K]) int {
			switch {
			case a.Key < b.Key:
				return -1
			case a.Key > b.Key:
				return 1
			default:
				return 0
			}
		})
	}

	if len(keys) > b.order {
		mid := (b.order + 1) / 2
		left := leaf(keys[:mid])
		if err := b.store.Save(ctx, left); err != nil {
			return nil, fmt.Errorf("split node: %w", err)
		}
		right := leaf(keys[mid:])
		if err := b.store.Save(ctx, right); err != nil {
			return nil, fmt.Errorf("split node: %w", err)
		}

		return []*Ref[K]{
			{
				From: nil,
				To:   &keys[mid].Key,
				N:    left.ID(),
			},
			{
				From: &keys[mid].Key,
				To:   nil,
				N:    right.ID(),
			},
		}, nil
	}

	if len(refs) > b.order {
		mid := (b.order + 1) / 2
		left := nonLeaf(refs[:mid])
		if err := b.store.Save(ctx, left); err != nil {
			return nil, fmt.Errorf("split node: %w", err)
		}

		right := nonLeaf(refs[mid:])
		if err := b.store.Save(ctx, right); err != nil {
			return nil, fmt.Errorf("split node: %w", err)
		}

		return []*Ref[K]{
			{
				From: nil,
				To:   refs[mid].From,
				N:    left.ID(),
			},
			{
				From: refs[mid].From,
				To:   nil,
				N:    right.ID(),
			},
		}, nil
	}

	n.SetKeys(keys)
	n.SetRefs(refs)
	b.store.Save(ctx, n)
	return movingUp, nil
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
