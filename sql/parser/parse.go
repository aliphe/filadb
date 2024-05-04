package parser

import (
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/aliphe/filadb/sql/lexer"
)

var (
	ErrUnexpectedEndOfInput = errors.New("unexpexted end of input")
)

type UnexpectedTokenError struct {
	token *lexer.Token
}

func (u UnexpectedTokenError) Error() string {
	return fmt.Sprintf("unexpected token %s at position %d", u.token.Value, u.token.Position)
}

type SQLQuery struct {
	Select
}

type Select struct {
	Fields []Field
	From
	Where *Where
}

type Field struct {
	Column string
}

type From struct {
	Table string
}

type Where struct {
	Filter
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
	expr := newExpr(tokens)

	sel, cur, err := parseSelect(expr)
	if err != nil {
		return SQLQuery{}, err
	}
	if cur.cursor < len(expr.tokens)-1 {
		return SQLQuery{}, UnexpectedTokenError{tokens[cur.cursor]}
	}

	// TODO return the biggest cursor of select/insert
	return SQLQuery{
		Select: sel,
	}, nil
}

func parseSelect(in *expr) (Select, *expr, error) {
	cur, expr, err := in.Read(1)
	if errors.Is(err, io.EOF) {
		return Select{}, in, ErrUnexpectedEndOfInput
	}
	if cur[0].Kind != lexer.KindSelect {
		return Select{}, in, UnexpectedTokenError{cur[0]}
	}

	fields, expr, err := parseFields(expr)
	if err != nil {
		return Select{}, in, fmt.Errorf("parse select fields: %w", err)
	}

	from, expr, err := parseFrom(expr)
	if err != nil {
		return Select{}, in, fmt.Errorf("parse from: %w", err)
	}

	where, selExpr, err := parseWhere(expr)
	// where is optional
	if err != nil && !errors.Is(err, ErrUnexpectedEndOfInput) {
		return Select{}, in, fmt.Errorf("parse where: %w", err)
	} else if err == nil {
		expr = selExpr
	}
	log.Println("bah")

	return Select{
		Fields: fields,
		From:   from,
		Where:  where,
	}, expr, nil
}

func parseFields(in *expr) ([]Field, *expr, error) {
	cur, expr, err := in.ExpectRead(1, lexer.KindLiteral)
	if err != nil {
		return nil, nil, err
	}

	var fields = []Field{
		{
			Column: cur[0].Value,
		},
	}

	for {
		cur, exp, err := expr.Read(2)
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
	_, expr, err := in.ExpectRead(1, lexer.KindFrom)
	if err != nil {
		return From{}, nil, err
	}

	cur, expr, err := expr.ExpectRead(1, lexer.KindLiteral)
	if err != nil {
		return From{}, nil, err
	}

	return From{
		Table: cur[0].Value,
	}, expr, nil
}

func parseWhere(in *expr) (*Where, *expr, error) {
	log.Println("parseWhere")
	_, expr, err := in.ExpectRead(1, lexer.KindWhere)
	if err != nil {
		return nil, nil, err
	}
	log.Println("read where")

	filter, expr, err := parseFilter(expr)
	if err != nil {
		return nil, nil, err
	}
	log.Println("read filter")

	return &Where{
		Filter: filter,
	}, expr, nil
}

func parseFilter(in *expr) (Filter, *expr, error) {
	cur, expr, err := in.Read(3)
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
