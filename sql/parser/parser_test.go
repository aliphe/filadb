package parser

import (
	"encoding/json"
	"log"
	"testing"

	"github.com/aliphe/filadb/sql/lexer"
)

func Test_ParseExpr(t *testing.T) {
	tests := map[string]struct {
		want  SQLQuery
		valid bool
	}{
		"SELECT * FROM users WHERE id = 1;": {
			want:  SQLQuery{},
			valid: true,
		},
	}

	for expr := range tests {
		t.Run(expr, func(t *testing.T) {
			tokens, err := lexer.Tokenize(expr)
			if err != nil {
				t.Fatal(err)
			}

			got, err := Parse(tokens)
			if err != nil {
				t.Fatal(err)
			}
			b, _ := json.Marshal(got)
			log.Printf("%s", b)
		})
	}
}
