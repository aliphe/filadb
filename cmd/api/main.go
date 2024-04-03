package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"github.com/aliphe/filadb/cmd/api/router"
	"github.com/aliphe/filadb/db"
)

var (
	version = flag.String("version", "0.0.1", "version of the service")
)

func main() {
	flag.Parse()
	f, err := os.OpenFile("db.txt", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(fmt.Errorf("open file: %w", err))
	}

	db := db.New(f)
	r := router.Init(db, router.WithVersion(*version))

	slog.Info("http server ready", slog.String("port", "3000"))
	http.ListenAndServe(":3000", r)
}
