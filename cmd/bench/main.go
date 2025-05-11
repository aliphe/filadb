package main

import (
	"context"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"github.com/aliphe/filadb/btree/file"
	"github.com/aliphe/filadb/cmd/bench/scenario"
	"github.com/aliphe/filadb/cmd/db/app"
	"github.com/aliphe/filadb/cmd/db/app/handler"
	fnet "github.com/aliphe/filadb/net"
)

func main() {
	// initialise a listener on a random port to retrieve a valid one.
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}
	addr := listener.Addr().String()
	listener.Close()
	ctx := context.Background()

	// init temp dir
	dir, err := os.MkdirTemp(os.TempDir(), "bench-*")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(dir)
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			log.Fatalf("failed to remove dir %s: %s", dir, err)
		}
	}()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go app.Run(ctx, app.WithFileOptions(file.WithPath(dir)), app.WithHandlerOptions(handler.WithAddr(addr)))

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	if err := run(conn, scenario.Basic); err != nil {
		log.Fatal(err)
	}
}

func run(conn net.Conn, scenario string) error {
	log.Println("starting scenario...")
	start := time.Now()
	for q := range strings.SplitSeq(scenario, ";") {
		if q == "" || q == "\n" {
			continue
		}
		err := fnet.Write(conn, []byte(q))
		if err != nil {
			return err
		}

		_, err = fnet.Read(conn)
		if err != nil {
			return err
		}
	}

	log.Printf("took %dms", time.Now().Sub(start).Milliseconds())
	return nil
}
