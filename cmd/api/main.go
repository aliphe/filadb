package main

import (
	"flag"
	"log/slog"
	"net/http"
	"os"

	"github.com/aliphe/filadb/btree"
	"github.com/aliphe/filadb/btree/file"
	"github.com/aliphe/filadb/cmd/api/router"
	"github.com/aliphe/filadb/db"
)

var (
	version = flag.String("version", "0.0.1", "version of the service")
)

func main() {
	flag.Parse()

	err := os.MkdirAll(".db", os.ModePerm)
	if err != nil {
		panic(err)
	}

	f, err := os.Open(".db")
	if err != nil {
		panic(err)
	}
	fileStore, err := file.New[string](f)
	if err != nil {
		panic(err)
	}
	btree := btree.New[string](500, fileStore)

	db := db.New(btree)
	r := router.Init(db, router.WithVersion(*version))

	slog.Info("http server ready", slog.String("port", "3000"))
	if err := http.ListenAndServe(":3000", r); err != nil {
		panic(err)
	}
}
