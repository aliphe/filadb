package parser

import (
	"errors"
	"fmt"
	"io"

	"github.com/aliphe/filadb/db/object"
	"github.com/aliphe/filadb/db/schema"
	"github.com/aliphe/filadb/sql/lexer"
)

var (
	ErrEndOfInput = errors.New("unexpexted end of input")
)

type UnexpectedTokenError struct {
	token *lexer.Token
}

func (u UnexpectedTokenError) Error() string {
	return fmt.Sprintf("unexpected token \"%s\" at position %d", u.token.Value, u.token.Position)
}

type QueryType string

const (
	QueryTypeSelect      QueryType = "select"
	QueryTypeInsert      QueryType = "insert"
	QueryTypeCreateTable QueryType = "create table"
)

type SQLQuery struct {
	Type        QueryType
	Select      Select
	Insert      Insert
	CreateTable CreateTable
}

type CreateTable struct {
	Name    string
	Columns []schema.Property
}

type Insert struct {
	Table string
	Rows  []object.Row
}

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
	if cur[0].Kind == lexer.KindSelect {
		sel, exp, err := parseSelect(expr)
		if err != nil {
			return SQLQuery{}, err
		}
		out.Select = sel
		out.Type = QueryTypeSelect
		expr = exp
	} else if cur[0].Kind == lexer.KindInsert {
		ins, exp, err := parseInsert(expr)
		if err != nil {
			return SQLQuery{}, err
		}
		out.Insert = ins
		out.Type = QueryTypeInsert
		expr = exp
	} else if cur[0].Kind == lexer.KindCreate {
		create, exp, err := parseCreateTable(expr)
		if err != nil {
			return SQLQuery{}, err
		}
		out.CreateTable = create
		out.Type = QueryTypeCreateTable
		expr = exp
	} else {
		return SQLQuery{}, UnexpectedTokenError{cur[0]}
	}
	_, exp, err := expr.expectRead(1, lexer.KindSemiColumn)
	if err != nil {
		if !errors.Is(err, ErrEndOfInput) {
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

func parseCreateTable(in *expr) (CreateTable, *expr, error) {
	_, expr, err := in.expectRead(1, lexer.KindTable)
	if err != nil {
		return CreateTable{}, nil, err
	}

	cur, expr, err := expr.expectRead(1, lexer.KindIdentifier)
	if err != nil {
		return CreateTable{}, nil, err
	}
	name := cur[0].Value

	cols, expr, err := parseKeyValuePairs(expr)
	if err != nil {
		return CreateTable{}, nil, err
	}
	return CreateTable{
		Name:    name,
		Columns: cols,
	}, expr, nil
}

func parseInsert(in *expr) (Insert, *expr, error) {
	cur, expr, err := in.expectRead(2, lexer.KindInto, lexer.KindIdentifier)
	if err != nil {
		return Insert{}, nil, fmt.Errorf("parse insert: %w", err)
	}
	table := cur[1].Value

	cols, expr, err := parseCols(expr)
	if err != nil {
		return Insert{}, nil, fmt.Errorf("parse insert columns: %w", err)
	}

	_, expr, err = expr.expectRead(1, lexer.KindValues)
	if err != nil {
		return Insert{}, nil, err
	}
	values, expr, err := parseValues(expr, cols)
	if err != nil {
		return Insert{}, nil, fmt.Errorf("parse insert values: %w", err)
	}

	return Insert{
		Table: table,
		Rows:  values,
	}, expr, nil
}

func parseCols(in *expr) ([]string, *expr, error) {
	col, expr, err := parseCSV(in)
	if err != nil {
		return nil, nil, fmt.Errorf("parse insert columns: %w", err)
	}

	cols := make([]string, 0, len(col))
	for _, c := range col {
		cols = append(cols, c.(string))
	}

	return cols, expr, nil
}

func parseCSV(in *expr) ([]interface{}, *expr, error) {
	_, expr, err := in.expectRead(1, lexer.KindOpenParen)
	if err != nil {
		return nil, nil, err
	}
	var row []interface{}
	for {
		cur, exp, err := expr.read(2)
		if err != nil {
			return nil, nil, err
		}
		expr = exp
		row = append(row, cur[0].Value)

		if cur[1].Kind == lexer.KindCloseParen {
			break
		}
		if cur[1].Kind != lexer.KindComma {
			return nil, nil, UnexpectedTokenError{cur[1]}
		}
	}
	return row, expr, nil
}

func parseKeyValuePairs(in *expr) ([]schema.Property, *expr, error) {
	_, expr, err := in.expectRead(1, lexer.KindOpenParen)
	if err != nil {
		return nil, nil, err
	}
	var out []schema.Property
	for {
		cur, exp, err := expr.read(3)
		if err != nil {
			return nil, nil, err
		}
		if cur[0].Kind != lexer.KindIdentifier {
			return nil, nil, UnexpectedTokenError{cur[0]}
		}

		expr = exp
		prop := schema.Property{
			Name: cur[0].Value,
		}
		if cur[1].Kind == lexer.KindText {
			prop.Type = schema.PropertyTypeText
		} else if cur[1].Kind == lexer.KindNumber {
			prop.Type = schema.PropertyTypeNumber
		} else {
			return nil, nil, UnexpectedTokenError{cur[1]}
		}
		out = append(out, prop)

		if cur[2].Kind == lexer.KindCloseParen {
			break
		}
		if cur[2].Kind != lexer.KindComma {
			return nil, nil, UnexpectedTokenError{cur[1]}
		}
	}
	return out, expr, nil
}

func parseValues(in *expr, cols []string) ([]object.Row, *expr, error) {
	vals, expr, err := parseCSV(in)
	if err != nil {
		return nil, nil, err
	}

	row := make(object.Row, 0)
	for i := range cols {
		row[cols[i]] = vals[i]
	}

	cur, exp, err := expr.read(1)
	if err != nil {
		if errors.Is(err, ErrEndOfInput) {
			// return the last one
			return []object.Row{row}, expr, nil
		}
		return nil, nil, err
	}

	// append the next values row
	if cur[0].Kind == lexer.KindComma {
		r, expr, err := parseValues(exp, cols)
		if err != nil {
			return nil, nil, err
		}
		return append(r, row), expr, nil
	}

	// return the last one
	return []object.Row{row}, exp, nil
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
	cur, expr, err := in.expectRead(1, lexer.KindIdentifier)
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
			cur[0].Kind != lexer.KindComma || cur[1].Kind != lexer.KindIdentifier {
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

	cur, expr, err := expr.expectRead(1, lexer.KindIdentifier)
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
	if cur[0].Kind != lexer.KindIdentifier {
		return Filter{}, nil, UnexpectedTokenError{cur[0]}
	}
	if cur[2].Kind != lexer.KindIdentifier {
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
