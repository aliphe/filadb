package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"

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

	addr := uri.Address()
	fmt.Println(addr)

	conn, err := net.Dial("tcp", uri.Address())
	if err != nil {
		log.Fatalf("connecting to database: %s", err)
	}

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("> ")
		query, err := reader.ReadString(';')
		if err != nil {
			if errors.Is(io.EOF, err) {
				break
			}
			log.Fatalf("reading input: %s", err)
		}

		err = fnet.Write(conn, []byte(query))
		if err != nil {
			log.Fatalf("sending query: %s", err)
		}

		res, err := fnet.Read(conn)
		if err != nil {
			log.Fatalf("reading response: %s", err)
		}

		fmt.Println(string(res))
	}
}
