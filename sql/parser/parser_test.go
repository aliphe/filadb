package parser

import (
	"encoding/json"
	"log"
	"testing"

	"github.com/aliphe/filadb/sql/lexer"
)

func Test_ParseExpr(t *testing.T) {
	tests := map[string]struct {
		given []*lexer.Token
		want  SQLQuery
		valid bool
	}{
		"SELECT * FROM users;": {
			given: []*lexer.Token{
				{
					Kind:  lexer.KindSelect,
					Value: "SELECT",
				},
				{
					Kind:  lexer.KindLiteral,
					Value: "*",
				},
				{
					Kind:  lexer.KindFrom,
					Value: "FROM",
				},
				{
					Kind:  lexer.KindLiteral,
					Value: "users",
				},
				{
					Kind:  lexer.KindWhere,
					Value: "WHERE",
				},
				{
					Kind:  lexer.KindLiteral,
					Value: "id",
				},
				{
					Kind:  lexer.KindEqual,
					Value: "=",
				},
				{
					Kind:  lexer.KindLiteral,
					Value: "uuid",
				},
				{
					Kind:  lexer.KindSemiColumn,
					Value: ";",
				},
			},
			want:  SQLQuery{},
			valid: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := Parse(tc.given)
			if err != nil {
				log.Println(err)
			}
			b, _ := json.Marshal(got)
			log.Printf("%s", b)
		})
	}
}
