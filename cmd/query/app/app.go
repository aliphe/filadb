package app

import (
	"fmt"

	"github.com/aliphe/filadb/btree"
	"github.com/aliphe/filadb/btree/file"
	"github.com/aliphe/filadb/cmd/query/app/factory"
	"github.com/aliphe/filadb/cmd/query/app/handler"
	"github.com/aliphe/filadb/db"
	"github.com/aliphe/filadb/db/schema/marshaler"
	"github.com/aliphe/filadb/db/schema/registry"
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

func Run(opts ...Option) error {
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

	schema, err := registry.New(btree, marshaler.New)
	if err != nil {
		return err
	}

	db := db.NewClient(btree, schema)
	q := sql.NewRunner(db)

	handler, err := factory.NewHandler(q, handler.TypeTCP)
	if err != nil {
		return fmt.Errorf("init request handler: %w", err)
	}

	if err := handler.Listen(); err != nil {
		return fmt.Errorf("listening to request: %w", err)
	}

	return nil
}
