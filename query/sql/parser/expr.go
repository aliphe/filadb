package parser

import "github.com/aliphe/filadb/query/sql/lexer"

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
		return nil, nil, ErrEndOfInput
	}
	return e.tokens[0:n], &expr{
		tokens: e.tokens[n:],
		cursor: e.cursor + n,
	}, nil
}

func (e *expr) read(n int, assertion assertion) ([]*lexer.Token, *expr, error) {
	toks, expr, err := e.r(n)
	if err != nil {
		return nil, nil, err
	}

	err, _ = assertion(toks...)

	return toks, expr, err
}

func (e *expr) Cursor() int {
	return e.cursor
}

type assertion func(t ...*lexer.Token) (error, []lexer.Kind)

func is(k lexer.Kind) assertion {
	return func(t ...*lexer.Token) (error, []lexer.Kind) {
		if k != lexer.KindAny && t[0].Kind != k {
			return newUnexpectedTokenError(t[0], k), []lexer.Kind{k}
		}
		return nil, nil
	}
}

func sequence(a ...assertion) assertion {
	return func(t ...*lexer.Token) (error, []lexer.Kind) {
		for i, a := range a {
			err, _ := a(t[i])
			if err != nil {
				return err, nil
			}
		}
		return nil, nil
	}
}

func oneOf(a ...assertion) assertion {
	return func(t ...*lexer.Token) (error, []lexer.Kind) {
		var want []lexer.Kind
		for _, a := range a {
			err, k := a(t[0])
			if err == nil {
				return nil, nil
			}
			want = append(want, k...)
		}
		return nil, dedup(want)
	}
}

func dedup[S comparable](s []S) []S {
	kv := make(map[S]bool, len(s))
	for _, k := range s {
		kv[k] = true
	}

	out := make([]S, len(kv))
	for k := range kv {
		out = append(out, k)
	}
	return out
}
