package parser

import (
	"github.com/aliphe/filadb/sql/lexer"
)

type expr struct {
	tokens []*lexer.Token
	cursor int
}

func newExpr(tokens []*lexer.Token) *expr {
	return &expr{
		tokens: clearWhitespaces(tokens),
		cursor: 0,
	}
}

func clearWhitespaces(tokens []*lexer.Token) []*lexer.Token {
	toks := make([]*lexer.Token, 0, len(tokens)/2)
	for _, t := range tokens {
		if t.Kind != lexer.KindWhitespace {
			toks = append(toks, t)
		}
	}
	return toks
}

func (e *expr) Read(n int) ([]*lexer.Token, *expr, error) {
	if n > len(e.tokens) {
		return nil, nil, ErrUnexpectedEndOfInput
	}
	return e.tokens[0:n], &expr{
		tokens: e.tokens[n:],
		cursor: e.cursor + n,
	}, nil
}

func (e *expr) ExpectRead(n int, kinds ...lexer.Kind) ([]*lexer.Token, *expr, error) {
	toks, expr, err := e.Read(n)
	if err != nil {
		return nil, nil, err
	}
	if len(toks) < len(kinds) {
		return nil, nil, UnexpectedTokenError{toks[len(kinds)]}
	}
	for i := 0; i < len(kinds) && i < n; i++ {
		if toks[i].Kind != kinds[i] {
			return nil, nil, UnexpectedTokenError{toks[i]}
		}
	}

	return toks, expr, nil
}

func (e *expr) Cursor() int {
	return e.cursor
}