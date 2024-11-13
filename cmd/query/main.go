package main

import (
	"github.com/aliphe/filadb/cmd/query/app"
)

func main() {
	if err := app.Run(); err != nil {
		panic(err)
	}
}
