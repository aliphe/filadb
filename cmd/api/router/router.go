package router

import (
	"net/http"

	"github.com/aliphe/filadb/db"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

func Init(db *db.Client, opts ...Option) *chi.Mux {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}

	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Get("/version", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(o.version))
	})

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("OK"))
	})

	r.Post("/query", query(db))

	return r
}
