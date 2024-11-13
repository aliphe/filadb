package factory

import (
	"fmt"

	"github.com/aliphe/filadb/cmd/query/app/handler"
	"github.com/aliphe/filadb/cmd/query/app/restapi"
	"github.com/aliphe/filadb/cmd/query/app/tcp"
	"github.com/aliphe/filadb/query"
)

func NewHandler(q query.Runner, t handler.Type, opts ...handler.Option) (handler.Handler, error) {
	switch t {
	case handler.TypeRestAPI:
		return restapi.New(q, opts...), nil
	case handler.TypeTCP:
		return tcp.New(q, opts...), nil
	}
	return nil, fmt.Errorf("unhandled type: %s", t)
}
