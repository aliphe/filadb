package restapi

import (
	"net/http"

	"github.com/aliphe/filadb/cmd/query/app/handler"
	"github.com/aliphe/filadb/query"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

type Handler struct {
	r       query.Runner
	version string
	addr    string
}

func New(q query.Runner, opts ...handler.Option) *Handler {
	o := &handler.Options{
		Version: "0.1.0",
		Addr:    ":3000",
	}
	for _, opt := range opts {
		opt(o)
	}
	return &Handler{
		r:       q,
		version: o.Version,
		addr:    o.Addr,
	}
}

func (h *Handler) Listen() error {
	mux := h.initRouter()

	return http.ListenAndServe(h.addr, mux)
}

func (h *Handler) Close() {

}

func (h *Handler) initRouter() *chi.Mux {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Get("/version", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(h.version))
	})

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("OK"))
	})

	r.Post("/query", handle(h.r))

	return r
}
