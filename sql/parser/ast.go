package parser

import "github.com/aliphe/filadb/sql/lexer"

type Expr struct {
	tokens []*lexer.Token
	Select
}

type Select struct {
	tokens []*lexer.Token
}
