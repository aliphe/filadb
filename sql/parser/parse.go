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
	ErrEndOfInput = errors.New("unexpected end of input")
)

type UnexpectedTokenError struct {
	token *lexer.Token
	want  []lexer.Kind
}

func newUnexpectedTokenError(token *lexer.Token, want ...lexer.Kind) UnexpectedTokenError {
	return UnexpectedTokenError{token, want}
}

func (u UnexpectedTokenError) Error() string {
	return fmt.Sprintf("unexpected token \"%s\" at position %d, want one of %v", u.token.Value, u.token.Position, u.want)
}

type QueryType string

const (
	QueryTypeSelect      QueryType = "select"
	QueryTypeInsert      QueryType = "insert"
	QueryTypeUpdate      QueryType = "update"
	QueryTypeCreateTable QueryType = "create table"
)

type SQLQuery struct {
	Type        QueryType
	Select      Select
	Insert      Insert
	Update      Update
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

type Update struct {
	From From
	Set  Set
}

type Set struct {
	Update object.Row
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
	Value  interface{}
}

type Op int

const (
	OpEqual = iota
)

func Parse(tokens []*lexer.Token) (SQLQuery, error) {
	in := newExpr(tokens)
	out := SQLQuery{}

	cur, expr, err := in.read(1,
		oneOf(
			is(lexer.KindSelect),
			is(lexer.KindInsert),
			is(lexer.KindCreate),
			is(lexer.KindUpdate),
		))
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
	} else if cur[0].Kind == lexer.KindUpdate {
		up, exp, err := parseUpdate(expr)
		if err != nil {
			return SQLQuery{}, err
		}
		out.Update = up
		out.Type = QueryTypeUpdate
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
		return SQLQuery{}, newUnexpectedTokenError(cur[0], lexer.KindCreate, lexer.KindSelect, lexer.KindInsert)
	}
	_, exp, err := expr.read(1, is(lexer.KindSemiColumn))
	if err != nil {
		if !errors.Is(err, ErrEndOfInput) {
			return SQLQuery{}, err
		}
	} else {
		expr = exp
	}

	if expr.cursor < len(in.tokens)-1 {
		return SQLQuery{}, newUnexpectedTokenError(tokens[expr.cursor])
	}

	return out, nil
}

func parseUpdate(in *expr) (Update, *expr, error) {
	cur, expr, err := in.read(1, sequence(
		is(lexer.KindIdentifier),
	))
	if err != nil {
		return Update{}, nil, err
	}

	table, ok := cur[0].Value.(string)
	if !ok {
		return Update{}, nil, fmt.Errorf("invalid table name %v", cur[0].Value)
	}

	set, expr, err := parseSet(expr)
	if err != nil {
		return Update{}, nil, fmt.Errorf("parse set: %w", err)
	}

	where, expr, err := parseWhere(expr)
	if err != nil {
		return Update{}, nil, fmt.Errorf("parse where: %w", err)
	}

	return Update{
		From: From{
			Table: table,
			Where: where,
		},
		Set: set,
	}, expr, nil
}

func parseSet(in *expr) (Set, *expr, error) {
	_, expr, err := in.read(1, is(lexer.KindSet))
	if err != nil {
		return Set{}, nil, err
	}

	row, expr, err := parseRow(expr)
	if err != nil {
		return Set{}, nil, err
	}

	return Set{
		Update: row,
	}, expr, nil
}

func parseCreateTable(in *expr) (CreateTable, *expr, error) {
	cur, expr, err := in.read(2, sequence(is(lexer.KindTable), is(lexer.KindIdentifier)))
	if err != nil {
		return CreateTable{}, nil, err
	}

	name, ok := cur[1].Value.(string)
	if !ok {
		return CreateTable{}, nil, fmt.Errorf("invalid table name %v", cur[0].Value)
	}

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
	cur, expr, err := in.read(2, sequence(is(lexer.KindInto), is(lexer.KindIdentifier)))
	if err != nil {
		return Insert{}, nil, fmt.Errorf("parse insert: %w", err)
	}
	table, ok := cur[1].Value.(string)
	if !ok {
		return Insert{}, nil, fmt.Errorf("invalid table name %v", cur[1].Value)
	}

	cols, expr, err := parseCols(expr)
	if err != nil {
		return Insert{}, nil, fmt.Errorf("parse insert columns: %w", err)
	}

	_, expr, err = expr.read(1, is(lexer.KindValues))
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
	_, expr, err := in.read(1, is(lexer.KindOpenParen))
	if err != nil {
		return nil, nil, err
	}
	var row []interface{}
	for {
		cur, exp, err := expr.read(2, sequence(
			oneOf(is(lexer.KindNumberLiteral), is(lexer.KindStringLiteral)),
			oneOf(is(lexer.KindCloseParen), is(lexer.KindComma)),
		))
		if err != nil {
			return nil, nil, err
		}
		expr = exp
		row = append(row, cur[0].Value)

		if cur[1].Kind == lexer.KindCloseParen {
			break
		}
	}
	return row, expr, nil
}

func parseRow(in *expr) (object.Row, *expr, error) {
	_, expr, err := in.read(1, is(lexer.KindOpenParen))
	if err != nil {
		return nil, nil, err
	}
	out := make(object.Row)
	for {
		cur, exp, err := expr.read(4,
			sequence(
				is(lexer.KindIdentifier),
				is(lexer.KindEqual),
				oneOf(is(lexer.KindText), is(lexer.KindNumber)),
				oneOf(is(lexer.KindCloseParen), is(lexer.KindComma)),
			))
		if err != nil {
			return nil, nil, err
		}

		expr = exp
		propName, ok := cur[0].Value.(string)
		if !ok {
			return nil, nil, fmt.Errorf("invalid property name %v", cur[0].Value)
		}
		out[propName] = cur[2].Value

		if cur[3].Kind == lexer.KindCloseParen {
			break
		}
	}
	return out, expr, nil
}

func parseKeyValuePairs(in *expr) ([]schema.Property, *expr, error) {
	_, expr, err := in.read(1, is(lexer.KindOpenParen))
	if err != nil {
		return nil, nil, err
	}
	var out []schema.Property
	for {
		cur, exp, err := expr.read(3,
			sequence(
				is(lexer.KindIdentifier),
				oneOf(is(lexer.KindText), is(lexer.KindNumber)),
				oneOf(is(lexer.KindCloseParen), is(lexer.KindComma)),
			))
		if err != nil {
			return nil, nil, err
		}

		expr = exp
		propName, ok := cur[0].Value.(string)
		if !ok {
			return nil, nil, fmt.Errorf("invalid property name %v", cur[0].Value)
		}
		prop := schema.Property{
			Name: propName,
		}
		if cur[1].Kind == lexer.KindText {
			prop.Type = schema.PropertyTypeText
		} else if cur[1].Kind == lexer.KindNumber {
			prop.Type = schema.PropertyTypeNumber
		} else {
			return nil, nil, newUnexpectedTokenError(cur[1], lexer.KindText, lexer.KindNumber)
		}
		out = append(out, prop)

		if cur[2].Kind == lexer.KindCloseParen {
			break
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

	cur, exp, err := expr.read(1, oneOf(
		is(lexer.KindComma), is(lexer.KindCloseParen),
	))
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
	cur, expr, err := in.read(1, is(lexer.KindIdentifier))
	if err != nil {
		return nil, nil, err
	}

	col, ok := cur[0].Value.(string)
	if !ok {
		return nil, nil, fmt.Errorf("invalid column name %v", cur[0].Value)
	}

	var fields = []Field{
		{
			Column: col,
		},
	}

	for {
		cur, exp, err := expr.read(2, sequence(
			is(lexer.KindComma),
			is(lexer.KindIdentifier),
		))
		if errors.Is(err, io.EOF) ||
			cur[0].Kind != lexer.KindComma || cur[1].Kind != lexer.KindIdentifier {
			break
		}

		expr = exp
		col, ok := cur[1].Value.(string)
		if !ok {
			return nil, nil, fmt.Errorf("invalid column name %v", cur[1].Value)
		}
		fields = append(fields, Field{Column: col})
	}

	return fields, expr, nil
}

func parseFrom(in *expr) (From, *expr, error) {
	_, expr, err := in.read(1, is(lexer.KindFrom))
	if err != nil {
		return From{}, nil, err
	}

	cur, expr, err := expr.read(1, is(lexer.KindIdentifier))
	if err != nil {
		return From{}, nil, err
	}

	where, expr, err := parseWhere(expr)
	if err != nil {
		return From{}, nil, err
	}

	table, ok := cur[0].Value.(string)
	if !ok {
		return From{}, nil, fmt.Errorf("invalid table name %b", cur[0].Value)
	}

	return From{
		Table: table,
		Where: where,
	}, expr, nil
}

func parseWhere(in *expr) (*Where, *expr, error) {
	_, expr, err := in.read(1, is(lexer.KindWhere))
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
		_, exp, err := expr.read(1, is(lexer.KindAnd))
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
	cur, expr, err := in.read(3, sequence(
		is(lexer.KindIdentifier),
		oneOf(is(lexer.KindEqual)),
		oneOf(is(lexer.KindStringLiteral), is(lexer.KindNumberLiteral)),
	))
	if err != nil {
		return Filter{}, nil, err
	}
	if cur[0].Kind != lexer.KindIdentifier {
		return Filter{}, nil, newUnexpectedTokenError(cur[0], lexer.KindIdentifier)
	}
	if cur[2].Kind != lexer.KindStringLiteral &&
		cur[2].Kind != lexer.KindNumberLiteral {
		return Filter{}, nil, newUnexpectedTokenError(cur[2], lexer.KindStringLiteral, lexer.KindNumberLiteral)
	}

	var op Op
	switch cur[1].Kind {
	case lexer.KindEqual:
		op = OpEqual
	default:
		return Filter{}, nil, newUnexpectedTokenError(cur[1], lexer.KindEqual)
	}
	col, ok := cur[0].Value.(string)
	if !ok {
		return Filter{}, nil, fmt.Errorf("invalid column name %v", cur[0].Value)
	}

	return Filter{
		Column: col,
		Op:     op,
		Value:  cur[2].Value,
	}, expr, nil
}
