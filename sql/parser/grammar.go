package parser

import "github.com/aliphe/filadb/sql/lexer"

// Expr = Select | Insert ";"
// Select = 'SELECT' Property (',' Property)* 'FROM' Table
// Property = (Table'.')? Column
// Column = Word
// Table = Word
// Word = [a-zA-Z_0-9]+

func ParseExpr(tokens []*lexer.Token) (Expr, bool) {
	if tokens[len(tokens)-1].Kind != lexer.KindSemiColumn {
		return Expr{}, false
	}

	sel, ok := ParseSelect(tokens[:len(tokens)-1])
	if !ok {
		return Expr{}, false
	}
	// TODO insert

	return Expr{
		tokens: tokens[len(tokens)-1:],
		Select: sel,
	}, true
}

func ParseSelect(tokens []*lexer.Token) (Select, bool) {
	var i int
	for i = range tokens {
		if tokens[i].Kind == lexer.KindFrom {
			break
		}
	}
	// KindFrom need to be found at any of the slots between
	if i >= len(tokens)-1 {
		return Select{}, false
	}

	return Select{tokens: tokens}, true
}
