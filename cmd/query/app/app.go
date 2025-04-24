package app

import (
	"context"
	"fmt"

	"github.com/aliphe/filadb/btree"
	"github.com/aliphe/filadb/btree/file"
	"github.com/aliphe/filadb/cmd/query/app/tcp"
	"github.com/aliphe/filadb/db"
	"github.com/aliphe/filadb/db/system"
	"github.com/aliphe/filadb/query/sql"
)

type options struct {
	fileOpts []file.Option
}

type Option func(*options)

func WithFileOptions(opts ...file.Option) Option {
	return func(o *options) {
		o.fileOpts = opts
	}
}

func Run(ctx context.Context, opts ...Option) error {
	var opt options
	for _, o := range opts {
		o(&opt)
	}

	fileStore, err := file.New[string](opt.fileOpts...)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := fileStore.Close(); err != nil {
			panic(err)
		}
	}()
	btree := btree.New(fileStore)

	schema := system.NewSchemaRegistry(btree)
	index := system.NewIndexRegistry(btree)

	db := db.NewClient(btree, schema, index)
	q := sql.NewRunner(db)

	server, err := tcp.NewServer(q)
	if err != nil {
		return err
	}

	errChan := make(chan error, 1)
	go func() {
		if err := server.Listen(ctx); err != nil {
			errChan <- fmt.Errorf("listening to request: %w", err)
		}
		close(errChan)
	}()

	select {
	case err := <-errChan:
		return err
	case <-ctx.Done():
		server.Close()
		return ctx.Err()
	}
}
