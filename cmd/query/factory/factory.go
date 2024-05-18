package factory

import (
	"github.com/aliphe/filadb/cmd/query/handler"
	"github.com/aliphe/filadb/cmd/query/restapi"
	"github.com/aliphe/filadb/query"
)

func NewHandler(q query.Runner, t handler.Type, opts ...handler.Option) handler.Handler {
	switch t {
	case handler.TypeRestAPI:
		return restapi.New(q, opts...)
	}
	return nil
}
