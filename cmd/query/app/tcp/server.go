package tcp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/aliphe/filadb/cmd/query/app/handler"
	"github.com/aliphe/filadb/query"
)

type Server struct {
	q       query.Runner
	timeout time.Duration
	l       net.Listener
	wg      sync.WaitGroup
	quit    chan any
}

func NewServer(q query.Runner, opts ...handler.Option) (*Server, error) {
	o := &handler.Options{
		Addr: ":5432",
	}
	for _, opt := range opts {
		opt(o)
	}

	ln, err := net.Listen("tcp", o.Addr)
	if err != nil {
		return nil, fmt.Errorf("init tcp connection: %w", err)
	}

	s := &Server{
		q:       q,
		l:       ln,
		timeout: o.Timeout,
		quit:    make(chan any),
	}

	s.wg.Add(1)
	return s, nil
}

func (s *Server) Listen(ctx context.Context) error {
	defer s.wg.Done()

	for {
		conn, err := s.l.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return nil
			default:
				slog.Error("accept connection", slog.Any("err", err))
			}
		}

		s.wg.Add(1)
		go func() {
			s.handleClient(conn)
			s.wg.Done()
		}()
	}
}

func (s *Server) Close() {
	close(s.quit)
	s.l.Close()
	s.wg.Wait()
}

func (s *Server) handleClient(conn net.Conn) {
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
			out, err := s.handleRequest(q)
			if err != nil {
				fmt.Fprintf(conn, "handle query: %s", err)
				continue
			}

			_, err = conn.Write(append(out, []byte(">")...))
			if err != nil {
				slog.Error("write response", slog.Any("err", err))
				return
			}
		}
	}
}

func (s *Server) handleRequest(q string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	slog.Info("received", slog.String("query", string(q)))

	res, err := s.q.Run(ctx, string(q))
	if err != nil {
		return fmt.Appendf(nil, "run sql query: %s\n", err), nil
	}

	if len(res) == 0 {
		return nil, nil
	}

	return res, nil
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
