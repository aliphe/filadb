package restapi

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/aliphe/filadb/db/csv"
	"github.com/aliphe/filadb/query"
)

func handle(q query.Runner) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		w.Header().Add("content-type", "text/csv")
		body, err := io.ReadAll(r.Body)
		if err != nil {
			if errors.Is(err, io.EOF) {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprint(w, "body is required")
				return
			} else {
				fmt.Fprintf(w, "parsing body: %s", err)
			}
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		query := string(body)
		slog.InfoContext(ctx, query)

		res, err := q.Run(ctx, query)
		if err != nil {
			fmt.Fprintf(w, "run sql query: %s", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		if len(res) == 0 {
			w.Write([]byte("[]"))
			return
		}
		csv := csv.NewWriter(w)
		err = csv.Write(res)
		if err != nil {
			fmt.Fprintf(w, "marshall result: %s", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}
}
