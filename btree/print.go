package btree

import (
	"context"
	"fmt"
	"strings"

	"github.com/aliphe/filadb/db/storage"
)

func (b *BTree[K]) Print(ctx context.Context, node string) (string, error) {
	root, ok, err := b.root(context.Background(), NodeID(node))
	if err != nil {
		return "", fmt.Errorf("acquire root: %w", err)
	}
	if !ok {
		return "", storage.ErrTableNotFound
	}
	out, err := b.printNode(ctx, root)
	if err != nil {
		return "", err
	}

	return out, nil
}

func (b *BTree[K]) printNode(ctx context.Context, n *Node[K]) (string, error) {
	if n.Leaf() {
		var out []string
		for _, k := range n.keys {
			out = append(out, string(k.Val))
		}
		return strings.Join(out, ","), nil
	}

	children := make([]string, 0, len(n.refs))
	for _, c := range n.refs {
		node, _, err := b.store.Find(ctx, c.N)
		if err != nil {
			return "", err
		}
		sub, err := b.printNode(ctx, node)
		if err != nil {
			return "", err
		}
		from := "]-∞"
		to := "∞["
		if c.From != nil {
			from = fmt.Sprintf("[%v", *c.From)
		}
		if c.To != nil {
			to = fmt.Sprintf("%v[", *c.To)
		}
		children = append(children, fmt.Sprintf("%v;%v(%s)", from, to, sub))
	}
	return strings.Join(children, ""), nil
}
