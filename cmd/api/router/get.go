package router

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/aliphe/filadb/db"
	tab "github.com/aliphe/filadb/db/table"
)

func get(d *db.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		v := r.URL.Query()
		table := v.Get("table")
		if len(table) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "table is required")
			return
		}
		id := v.Get("id")
		if len(id) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "id is required")
			return
		}

		res, found, err := d.Get(r.Context(), table, id)
		if err != nil {
			if errors.Is(err, tab.ErrTableNotFound) {
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprintf(w, "table %s not found", table)
			}
			slog.ErrorContext(ctx, fmt.Sprintf("db get: %v", err))
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if !found {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		w.WriteHeader(http.StatusOK)
		out, err := json.Marshal(res)
		if err != nil {
			slog.ErrorContext(ctx, fmt.Sprintf("unmarshal response: %v, %v", err, res))
			w.WriteHeader(http.StatusInternalServerError)
		}
		fmt.Fprint(w, string(out))
	}
}
