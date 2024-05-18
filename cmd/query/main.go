package main

import (
	"flag"
	"log/slog"

	"github.com/aliphe/filadb/btree"
	"github.com/aliphe/filadb/btree/file"
	"github.com/aliphe/filadb/cmd/query/factory"
	"github.com/aliphe/filadb/cmd/query/handler"
	"github.com/aliphe/filadb/db"
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

	db := db.NewClient(btree)
	q := sql.NewRunner(db)

	handler := factory.NewHandler(q, handler.TypeRestAPI)

	slog.Info("http server ready", slog.String("port", "3000"))
	if err := handler.Listen(); err != nil {
		panic(err)
	}
}
