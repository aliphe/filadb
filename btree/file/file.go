package file

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/aliphe/filadb/btree"
)

type BtreeStore[K btree.Key] struct {
	dir *os.File
}

func New[K btree.Key](file *os.File) (*BtreeStore[K], error) {
	s, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("retrieve file info: %w", err)
	}
	if !s.IsDir() {
		return nil, fmt.Errorf("file %s: %w", file.Name(), ErrExpectedDirectory)
	}

	return &BtreeStore[K]{
		dir: file,
	}, nil
}

func (b *BtreeStore[K]) Save(ctx context.Context, n *btree.Node[K]) error {
	path := filepath.Join(b.dir.Name(), string(n.ID()))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("open node file: %w", err)
	}
	defer f.Close()

	err = save(f, n)
	if err != nil {
		return err
	}

	return nil
}

func (b *BtreeStore[K]) Find(ctx context.Context, id btree.NodeID) (*btree.Node[K], bool, error) {
	path := filepath.Join(b.dir.Name(), string(id))

	c, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("read node: %w", err)
	}

	var node node[K]
	if err := json.Unmarshal(c, &node); err != nil {
		return nil, false, fmt.Errorf("parse node file: %w", err)
	}
	return btree.NewNode[K](id, node.Keys, node.Refs), true, nil
}

func save[K btree.Key](f *os.File, n *btree.Node[K]) error {
	node := node[K]{
		Keys: n.Keys(),
		Refs: n.Refs(),
	}
	b, err := json.Marshal(node)
	if err != nil {
		return fmt.Errorf("marshal node: %w", err)
	}
	_, err = f.Write(b)
	if err != nil {
		return fmt.Errorf("write node to disk: %w", err)
	}
	return nil
}

type node[K btree.Key] struct {
	Keys []*btree.KeyVal[K] `json:"keys,omitempty"`
	Refs []*btree.Ref[K]    `json:"refs,omitempty"`
}
