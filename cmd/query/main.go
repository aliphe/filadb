package main

import (
	"context"
	"flag"
	"log/slog"

	"github.com/aliphe/filadb/cmd/query/app"
)

var (
	verbose = flag.Bool("verbose", false, "enable more verbose logging")
)

func main() {
	flag.Parse()

	if *verbose {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}

	if err := app.Run(context.Background()); err != nil {
		panic(err)
	}
}
