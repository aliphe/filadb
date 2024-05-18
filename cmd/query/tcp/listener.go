package tcp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	"github.com/aliphe/filadb/cmd/query/handler"
	"github.com/aliphe/filadb/db/csv"
	"github.com/aliphe/filadb/query"
)

type Listener struct {
	q       query.Runner
	version string
	addr    string
	timeout time.Duration
}

func New(q query.Runner, opts ...handler.Option) *Listener {
	o := &handler.Options{
		Version: "0.1.0",
		Addr:    ":5432",
		Timeout: 1 * time.Minute,
	}
	for _, opt := range opts {
		opt(o)
	}
	return &Listener{
		q:       q,
		version: o.Version,
		addr:    o.Addr,
		timeout: o.Timeout,
	}
}

func (l *Listener) Listen() error {
	ln, err := net.Listen("tcp", l.addr)
	if err != nil {
		return fmt.Errorf("init tcp connection: %w", err)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			slog.Error("accept connection", slog.Any("err", err))
		}

		go l.handleClient(conn)
	}

}

func (l *Listener) handleClient(conn net.Conn) {
	defer conn.Close()

	for {
		queries, err := readQueries(conn)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				fmt.Fprintf(conn, "handle query: %s", err)
			}
			return
		}

		for _, q := range queries {
			out, err := l.handleRequest(q)
			if err != nil {
				fmt.Fprintf(conn, "handle query: %s", err)
				continue
			}

			_, err = conn.Write(out)
			if err != nil {
				slog.Error("write response", slog.Any("err", err))
				return
			}
		}
	}
}

func (l *Listener) handleRequest(q string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), l.timeout)
	defer cancel()

	slog.Info("received", slog.String("query", string(q)))

	res, err := l.q.Run(ctx, string(q))
	if err != nil {
		return []byte(fmt.Sprintf("run sql query: %s", err)), nil
	}

	if len(res) == 0 {
		return nil, nil
	}

	var b bytes.Buffer
	csv := csv.NewWriter(&b)
	err = csv.Write(res)
	if err != nil {
		return []byte(fmt.Sprintf("marshall result: %s", err)), nil
	}

	return b.Bytes(), nil
}

func readQueries(r io.Reader) ([]string, error) {
	buf := make([]byte, 4084)

	n, err := r.Read(buf)
	if err != nil {
		return nil, err
	}

	var out []string
	var j = 0
	for i := range buf[:n] {
		if buf[i] == ';' {
			out = append(out, string(buf[j:i]))
			j = i + i
		}
	}
	return out, nil
}
