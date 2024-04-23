package parser

import (
	"log"
	"testing"

	"github.com/aliphe/filadb/sql/lexer"
)

func Test_ParseExpr(t *testing.T) {
	tests := map[string]struct {
		given []*lexer.Token
		want  Expr
		valid bool
	}{
		"SELECT * FROM users;": {
			given: []*lexer.Token{
				{
					Kind: lexer.KindSelect,
				},
				{
					Kind: lexer.KindLiteral,
				},
				{
					Kind: lexer.KindFrom,
				},
				{
					Kind: lexer.KindLiteral,
				},
				{
					Kind: lexer.KindSemiColumn,
				},
			},
			want: Expr{
				tokens: []*lexer.Token{{Kind: lexer.KindSemiColumn}},
				Select: Select{
					tokens: []*lexer.Token{
						{
							Kind: lexer.KindSelect,
						},
						{
							Kind: lexer.KindLiteral,
						},
						{
							Kind: lexer.KindFrom,
						},
						{
							Kind: lexer.KindLiteral,
						},
					},
				},
			},
			valid: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got, ok := ParseExpr(tc.given)
			if ok != tc.valid {
				t.Fatalf("ParseExpr() mismatch, want isValid %v, got %v", tc.valid, ok)
			}
			log.Printf("%+v", got)
		})
	}
}
