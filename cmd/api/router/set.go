package router

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/aliphe/filadb/db"
)

type setQuery struct {
	Table string                 `json:"table"`
	Id    string                 `json:"id"`
	Row   map[string]interface{} `json:"row"`
}

func set(db *db.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		body, err := io.ReadAll(r.Body)
		if err != nil {
			if errors.Is(err, io.EOF) {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprint(w, "body is required")
				return
			}
			slog.ErrorContext(ctx, fmt.Sprintf("parse body: %b", err))
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		slog.InfoContext(ctx, string(body))
		q := setQuery{}
		if err := json.Unmarshal(body, &q); err != nil {
			slog.ErrorContext(ctx, fmt.Sprintf("parse body: %b", err))
			w.WriteHeader(http.StatusInternalServerError)
		}

		err = db.Insert(r.Context(), q.Table, q.Id, q.Row)
		if err != nil {
			slog.ErrorContext(ctx, fmt.Sprintf("db set: %v", err))
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
	}
}
