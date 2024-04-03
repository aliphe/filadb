package main

import (
	"flag"
	"log/slog"
	"net/http"

	"github.com/aliphe/filadb/cmd/api/router"
	"github.com/aliphe/filadb/db"
)

var (
	version = flag.String("version", "0.0.1", "version of the service")
)

func main() {
	flag.Parse()

	db := db.New()
	r := router.Init(db, router.WithVersion(*version))

	slog.Info("http server ready", slog.String("port", "3000"))
	http.ListenAndServe(":3000", r)
}
