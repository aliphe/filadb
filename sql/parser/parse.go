package parser

import (
	"errors"
	"fmt"
	"io"

	"github.com/aliphe/filadb/sql/lexer"
)

var (
	ErrUnexpectedEndOfInput = errors.New("unexpexted end of input")
)

type UnexpectedTokenError struct {
	token *lexer.Token
}

func (u UnexpectedTokenError) Error() string {
	return fmt.Sprintf("unexpected token \"%s\" at position %d", u.token.Value, u.token.Position)
}

type QueryType string

const (
	QueryTypeSelect QueryType = "select"
	QueryTypeInsert QueryType = "insert"
)

type SQLQuery struct {
	Type   QueryType
	Select Select
	Insert Insert
}

type Insert struct{}

type Select struct {
	Fields []Field
	From   From
}

type Field struct {
	Column string
}

type From struct {
	Table string
	Where *Where
}

type Where struct {
	Filters []Filter
}

type Filter struct {
	Column string
	Op     Op
	Value  string
}

type Op int

const (
	OpEqual = iota
)

func Parse(tokens []*lexer.Token) (SQLQuery, error) {
	in := newExpr(tokens)
	out := SQLQuery{}

	cur, expr, err := in.read(1)
	if err != nil {
		return SQLQuery{}, err
	}
	switch cur[0].Kind {
	case lexer.KindSelect:
		{
			sel, exp, err := parseSelect(expr)
			if err != nil {
				return SQLQuery{}, err
			}
			out.Select = sel
			expr = exp
			out.Type = QueryTypeSelect
		}
	case lexer.KindInsert:
		{
			ins, exp, err := parseInsert(expr)
			if err != nil {
				return SQLQuery{}, err
			}
			out.Insert = ins
			expr = exp
			out.Type = QueryTypeInsert
		}
	}
	_, exp, err := expr.expectRead(1, lexer.KindSemiColumn)
	if err != nil {
		if !errors.Is(err, ErrUnexpectedEndOfInput) {
			return SQLQuery{}, err
		}
	} else {
		expr = exp
	}

	if expr.cursor < len(in.tokens)-1 {
		return SQLQuery{}, UnexpectedTokenError{tokens[expr.cursor]}
	}

	return out, nil
}

func parseInsert(_ *expr) (Insert, *expr, error) {
	return Insert{}, nil, errors.New("not implemented")
}

func parseSelect(in *expr) (Select, *expr, error) {
	fields, expr, err := parseFields(in)
	if err != nil {
		return Select{}, in, fmt.Errorf("parse select fields: %w", err)
	}

	from, expr, err := parseFrom(expr)
	if err != nil {
		return Select{}, in, fmt.Errorf("parse from: %w", err)
	}

	return Select{
		Fields: fields,
		From:   from,
	}, expr, nil
}

func parseFields(in *expr) ([]Field, *expr, error) {
	cur, expr, err := in.expectRead(1, lexer.KindLiteral)
	if err != nil {
		return nil, nil, err
	}

	var fields = []Field{
		{
			Column: cur[0].Value,
		},
	}

	for {
		cur, exp, err := expr.read(2)
		if errors.Is(err, io.EOF) ||
			cur[0].Kind != lexer.KindComma || cur[1].Kind != lexer.KindLiteral {
			break
		}

		expr = exp
		fields = append(fields, Field{Column: cur[1].Value})
	}

	return fields, expr, nil
}

func parseFrom(in *expr) (From, *expr, error) {
	_, expr, err := in.expectRead(1, lexer.KindFrom)
	if err != nil {
		return From{}, nil, err
	}

	cur, expr, err := expr.expectRead(1, lexer.KindLiteral)
	if err != nil {
		return From{}, nil, err
	}

	where, expr, err := parseWhere(expr)
	if err != nil {
		return From{}, nil, err
	}

	return From{
		Table: cur[0].Value,
		Where: where,
	}, expr, nil
}

func parseWhere(in *expr) (*Where, *expr, error) {
	_, expr, err := in.expectRead(1, lexer.KindWhere)
	if err != nil {
		return nil, in, nil
	}

	filter, expr, err := parseFilter(expr)
	if err != nil {
		return nil, nil, err
	}
	var filters []Filter
	filters = append(filters, filter)
	for {
		_, exp, err := expr.expectRead(1, lexer.KindAnd)
		if err != nil {
			break
		}
		filter, exp, err := parseFilter(exp)
		if err != nil {
			return nil, nil, err
		}
		filters = append(filters, filter)
		expr = exp
	}

	return &Where{
		Filters: filters,
	}, expr, nil
}

func parseFilter(in *expr) (Filter, *expr, error) {
	cur, expr, err := in.read(3)
	if err != nil {
		return Filter{}, nil, err
	}
	if cur[0].Kind != lexer.KindLiteral {
		return Filter{}, nil, UnexpectedTokenError{cur[0]}
	}
	if cur[2].Kind != lexer.KindLiteral {
		return Filter{}, nil, UnexpectedTokenError{cur[2]}
	}

	var op Op
	switch cur[1].Kind {
	case lexer.KindEqual:
		op = OpEqual
	default:
		return Filter{}, nil, UnexpectedTokenError{cur[1]}
	}

	return Filter{
		Column: cur[0].Value,
		Op:     op,
		Value:  cur[2].Value,
	}, expr, nil
}
