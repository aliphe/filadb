package router

import (
	"fmt"
	"io"
	"net/http"

	"github.com/aliphe/filadb/db"
	"github.com/aliphe/filadb/query"
)

func op(db *db.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q, err := io.ReadAll(r.Body)
		if err != nil {
			fmt.Fprintf(w, "decode body: %s", err.Error())
			w.WriteHeader(400)
		}
		defer func() {
			if err := r.Body.Close(); err != nil {
				panic(err)
			}
		}()

		query.Parse(string(q))

		fmt.Fprintf(w, "received: %s", string(q))
	}
}
