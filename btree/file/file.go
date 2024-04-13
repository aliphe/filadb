package file

import (
	"context"
	"fmt"
	"os"

	"github.com/aliphe/filadb/btree"
)

type btreeStore[K btree.Key] struct {
	dir *os.File
}

func New[K btree.Key](file *os.File) (*btreeStore[K], error) {
	s, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("retrieve file info: %w", err)
	}
	if !s.IsDir() {
		return nil, fmt.Errorf("file %s: %w", file.Name(), ErrExpectedDirectory)
	}

	return &btreeStore[K]{
		dir: file,
	}, nil
}

func (b *btreeStore[K]) Save(ctx context.Context, n *btree.Node[K]) error {
	return nil
}
