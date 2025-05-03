package parser

import (
	"io"

	"github.com/aliphe/filadb/query/sql/lexer"
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

func (e *expr) r(n int) ([]*lexer.Token, *expr, error) {
	if n > len(e.tokens) {
		return nil, nil, io.EOF
	}
	return e.tokens[0:n], &expr{
		tokens: e.tokens[n:],
		cursor: e.cursor + n,
	}, nil
}

func (e *expr) read(assertions ...assertion) ([]*lexer.Token, *expr, error) {
	toks, expr, err := e.r(len(assertions))
	if err != nil {
		return nil, nil, err
	}

	err = sequence(assertions...)(toks...)

	return toks, expr, err
}

func (e *expr) Cursor() int {
	return e.cursor
}

type assertion func(t ...*lexer.Token) error

func is(k lexer.Kind) assertion {
	return func(t ...*lexer.Token) error {
		if k != lexer.KindAny && t[0].Kind != k {
			return newUnexpectedTokenError(t[0], k)
		}
		return nil
	}
}

func sequence(a ...assertion) assertion {
	return func(t ...*lexer.Token) error {
		for i, a := range a {
			err := a(t[i])
			if err != nil {
				return err
			}
		}
		return nil
	}
}

func oneOf(a ...assertion) assertion {
	return func(t ...*lexer.Token) error {
		for _, a := range a {
			err := a(t[0])
			if err == nil {
				return nil
			}
		}

		return newUnexpectedTokenError(t[0])
	}
}
