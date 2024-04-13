package btree

import (
	"context"
	"fmt"
	"strings"
)

func (b *BTree[K]) Print() (string, error) {
	out, err := b.printNode(b.root, 0)
	if err != nil {
		return "", err
	}

	return out, nil
}

func (b *BTree[K]) printNode(n *Node[K], depth int) (string, error) {
	if n.Leaf() {
		vals := make([]string, 0, len(n.Keys()))
		for i, v := range n.Keys() {
			vals = append(vals, printVal(v.Key, depth, i == len(n.Keys())-1))
		}
		return strings.Join(vals, "\n"), nil
	}
	refs := make([]string, 0, len(n.Refs())*2)
	for _, r := range n.Refs() {
		refs = append(refs, printRef(r, depth))
		s, ok, err := b.store.Find(context.Background(), r.N)
		if !ok || err != nil {
			return "", fmt.Errorf("find node %v: %w", r.N, err)
		}

		out, err := b.printNode(s, depth+1)
		if err != nil {
			return "", err
		}
		refs = append(refs, out)
	}

	return strings.Join(refs, "\n"), nil
}

func printRef[K Key](r *Ref[K], depth int) string {
	var out string
	for i := 0; i < depth; i++ {
		out = out + "  "
	}
	if r.From != nil {
		return out + fmt.Sprintf("└── %v", *r.From)
	}
	return ""
}

func printVal[K Key](k K, depth int, isLast bool) string {
	var prefix string
	for i := 0; i <= depth; i++ {
		if i < depth {
			prefix = prefix + "  "
		} else if isLast {
			prefix = prefix + "└──"
		} else {
			prefix = prefix + "├──"
		}
	}

	return fmt.Sprintf("%s %v", prefix, k)
}
