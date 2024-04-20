package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"flag"
	"log/slog"
	"net/http"
	"os"

	"github.com/aliphe/filadb/btree"
	"github.com/aliphe/filadb/btree/file"
	"github.com/aliphe/filadb/cmd/api/router"
	"github.com/aliphe/filadb/db"
	"github.com/aliphe/filadb/db/schema"
	"github.com/aliphe/filadb/db/storage"
)

var (
	version = flag.String("version", "0.0.1", "version of the service")
)

func seed(rw storage.ReaderWriter) {
	sch := schema.Schema{
		Table: "users",
		Properties: []*schema.Property{
			{
				Name: "id",
				Type: schema.PropertyTypeText,
			},
			{
				Name: "name",
				Type: schema.PropertyTypeText,
			},
			{
				Name: "email",
				Type: schema.PropertyTypeText,
			},
		},
	}
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	_ = enc.Encode(sch)
	rw.Add(context.Background(), string(schema.InternalTableSchemas), "users", buf.Bytes())
}

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
	btree := btree.New(500, fileStore)
	seed(btree)
	schema := schema.NewReader(btree)

	db := db.NewClient(btree, *schema)
	r := router.Init(db, router.WithVersion(*version))

	slog.Info("http server ready", slog.String("port", "3000"))
	if err := http.ListenAndServe(":3000", r); err != nil {
		panic(err)
	}
}
