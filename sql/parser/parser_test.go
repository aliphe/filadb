package parser

import (
	"testing"

	"github.com/aliphe/filadb/sql/lexer"
)

func Test_Parse(t *testing.T) {
	tests := map[string]struct {
		given []lexer.Token
		want  Expr
	}{
		"SELECT * FROM users": {
			given: []lexer.Token{
				*lexer.NewToken(lexer.KindSelect, ""),
				*lexer.NewToken(lexer.KindLiteral, ""),
				*lexer.NewToken(lexer.KindFrom, ""),
				*lexer.NewToken(lexer.KindLiteral, ""),
			},
			want: Expr{
				Select: Select{
					Fields: []Field{
						{
							Source: "users",
							Col:    "*",
						},
					},
					Sources: []Source{
						{
							Name: "users",
							Filter: func(i interface{}) bool {
								return true
							},
						},
					},
				},
			},
		},
	}

	for name, _ := range tests {
		t.Run(name, func(t *testing.T) {
			t.Skip()
		})
	}
}
