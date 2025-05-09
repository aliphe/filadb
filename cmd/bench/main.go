package main

import (
	"flag"
	"log"
	"net"
	"strings"
	"time"

	"github.com/aliphe/filadb/cmd/bench/scenario"
	fnet "github.com/aliphe/filadb/net"
	"github.com/aliphe/filadb/uri"
)

var (
	database_uri = flag.String("database_uri", "filadb://127.0.0.1:5432", "uri of running filadb database instance")
)

func main() {
	flag.Parse()

	uri, err := uri.Parse(*database_uri)
	if err != nil {
		log.Fatal(err)
	}

	conn, err := net.Dial("tcp", uri.Address())
	if err != nil {
		log.Fatalf("connecting to database: %s", err)
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
