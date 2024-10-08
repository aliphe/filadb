package main

import (
	"flag"

	"github.com/aliphe/filadb/btree"
	"github.com/aliphe/filadb/btree/file"
	"github.com/aliphe/filadb/cmd/query/factory"
	"github.com/aliphe/filadb/cmd/query/handler"
	"github.com/aliphe/filadb/db"
	idxregistry "github.com/aliphe/filadb/db/index/registry"
	"github.com/aliphe/filadb/db/schema/marshaler"
	"github.com/aliphe/filadb/db/schema/registry"
	"github.com/aliphe/filadb/query/sql"
)

var (
	version = flag.String("version", "0.0.1", "version of the service")
)

func main() {
	flag.Parse()

	fileStore, err := file.New[string]()
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
		panic(err)
	}

	index, err := idxregistry.New(btree, marshaler.New)
	if err != nil {
		panic(err)
	}

	db := db.NewClient(btree, schema, index)
	q := sql.NewRunner(db)

	handler, err := factory.NewHandler(q, handler.TypeTCP)
	if err != nil {
		panic(err)
	}

	if err := handler.Listen(); err != nil {
		panic(err)
	}
}
