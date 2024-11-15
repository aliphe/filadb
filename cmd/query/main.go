package main

import (
	"flag"
	"log/slog"

	"github.com/aliphe/filadb/cmd/query/app"
)

var (
	verbose = flag.Bool("verbose", false, "enable more verbose logging")
)

func main() {
	flag.Parse()

	if *verbose == true {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}

	if err := app.Run(); err != nil {
		panic(err)
	}
}
