package parser

import (
	"errors"
	"fmt"
	"io"

	"github.com/aliphe/filadb/db"
	"github.com/aliphe/filadb/db/object"
	"github.com/aliphe/filadb/db/schema"
	"github.com/aliphe/filadb/query/sql/lexer"
)

type UnexpectedTokenError struct {
	token *lexer.Token
	want  []lexer.Kind
}

func newUnexpectedTokenError(token *lexer.Token, want ...lexer.Kind) UnexpectedTokenError {
	return UnexpectedTokenError{token, want}
}

func (u UnexpectedTokenError) Error() string {
	if len(u.want) > 0 {
		return fmt.Sprintf("unexpected token \"%v\" at position %d, want one of %v", u.token.Value, u.token.Position, u.want)
	}
	return fmt.Sprintf("unexpected token \"%v\" at position %d", u.token.Value, u.token.Position)
}

type QueryType string

const (
	QueryTypeSelect QueryType = "select"
	QueryTypeInsert QueryType = "insert"
	QueryTypeUpdate QueryType = "update"
	QueryTypeCreate QueryType = "create"
)

type CreateType string

const (
	CreateTypeTable CreateType = "create"
	CreateTypeIndex CreateType = "index"
)

type SQLQuery struct {
	Type   QueryType
	Select Select
	Insert Insert
	Update Update
	Create Create
}

func (s *SQLQuery) Tables() []object.Table {
	switch s.Type {
	case QueryTypeSelect:
		return s.Select.Tables()
	case QueryTypeInsert:
		return s.Insert.Tables()
	case QueryTypeUpdate:
		return s.Update.Tables()
	default:
		return nil
	}
}

type Create struct {
	Type        CreateType
	CreateTable CreateTable
	CreateIndex CreateIndex
}

type CreateTable struct {
	Name    object.Table
	Columns []schema.Column
}

type CreateIndex struct {
	Name   string
	Table  object.Table
	Fields []Field
}

type Insert struct {
	Table object.Table
	Rows  []object.Row
}

func (i *Insert) Tables() []object.Table {
	return []object.Table{i.Table}
}

type Update struct {
	From    object.Table
	Set     Set
	Filters []Filter
}

func (u *Update) Tables() []object.Table {
	return []object.Table{u.From}
}

type Set struct {
	Update object.Row
}

type Select struct {
	Fields  []Field
	From    object.Table
	Joins   []Join
	Filters []Filter
	Limit   Limit
}

type Limit struct {
	Limit *int32
}

func (l *Limit) Get() (int32, bool) {
	if l.Limit != nil {
		return *l.Limit, true
	}
	return 0, false
}

func (s *Select) Tables() []object.Table {
	out := make([]object.Table, 0, len(s.Joins)+1)
	out = append(out, s.From)
	for _, j := range s.Joins {
		out = append(out, j.Table)
	}

	return out
}

type Join struct {
	Table object.Table
	Alias object.Table
	On    On
}

type On struct {
	// Local is the column referenced on the local table.
	Local Field
	// Foreign is the column referenced on the foreign table.
	Foreign Field
}

type Field struct {
	Table  object.Table
	Column string
}

type Filter struct {
	Left  Value
	Op    db.Op
	Right Value
}

type Value struct {
	Type      ValueType
	Reference Field
	Value     any
}

type ValueType int

const (
	ValueTypeLitteral ValueType = iota + 1
	ValueTypeReference
	ValueTypeList
)

func Parse(tokens []*lexer.Token) (*SQLQuery, error) {
	in := newExpr(tokens)
	out := SQLQuery{}

	cur, expr, err := in.read(oneOf(
		is(lexer.KindSelect),
		is(lexer.KindInsert),
		is(lexer.KindCreate),
		is(lexer.KindUpdate),
	))
	if err != nil {
		return nil, err
	}
	if cur[0].Kind == lexer.KindSelect {
		sel, exp, err := parseSelect(expr)
		if err != nil {
			return nil, err
		}
		out.Select = sel
		out.Type = QueryTypeSelect
		expr = exp
	} else if cur[0].Kind == lexer.KindInsert {
		ins, exp, err := parseInsert(expr)
		if err != nil {
			return nil, err
		}
		out.Insert = ins
		out.Type = QueryTypeInsert
		expr = exp
	} else if cur[0].Kind == lexer.KindUpdate {
		up, exp, err := parseUpdate(expr)
		if err != nil {
			return nil, err
		}
		out.Update = up
		out.Type = QueryTypeUpdate
		expr = exp
	} else if cur[0].Kind == lexer.KindCreate {
		create, exp, err := parseCreate(expr)
		if err != nil {
			return nil, err
		}
		out.Create = create
		out.Type = QueryTypeCreate
		expr = exp
	} else {
		return nil, newUnexpectedTokenError(cur[0], lexer.KindCreate, lexer.KindSelect, lexer.KindInsert, lexer.KindUpdate)
	}
	_, exp, err := expr.read(is(lexer.KindSemiColumn))
	if err != nil {
		if !errors.Is(err, io.EOF) {
			return nil, err
		}
	} else {
		expr = exp
	}

	if expr.cursor < len(in.tokens)-1 {
		return nil, newUnexpectedTokenError(tokens[expr.cursor])
	}

	return &out, nil
}

func parseUpdate(in *expr) (Update, *expr, error) {
	cur, expr, err := in.read(
		is(lexer.KindIdentifier),
	)
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
		From:    object.Table(table),
		Set:     set,
		Filters: where,
	}, expr, nil
}

func parseSet(in *expr) (Set, *expr, error) {
	_, expr, err := in.read(is(lexer.KindSet))
	if err != nil {
		return Set{}, nil, err
	}

	row, expr, err := parseSetContent(expr)
	if err != nil {
		return Set{}, nil, err
	}

	return Set{
		Update: row,
	}, expr, nil
}

func parseSetContent(in *expr) (object.Row, *expr, error) {
	out := make(object.Row)
	it := in
	for {
		cur, exp, err := it.read(
			is(lexer.KindIdentifier),
			is(lexer.KindEqual),
			oneOf(is(lexer.KindNumberLiteral), is(lexer.KindStringLiteral)),
		)
		if err != nil {
			return nil, nil, err
		}
		out[cur[0].Value.(string)] = cur[2].Value
		it = exp

		cur, exp, err = it.r(1)
		if err != nil {
			return nil, nil, err
		}

		if cur[0].Kind != lexer.KindComma {
			break
		}
		it = exp
	}

	return out, it, nil
}

func parseCreate(in *expr) (Create, *expr, error) {
	var out Create

	cur, expr, err := in.read(oneOf(is(lexer.KindTable), is(lexer.KindIndex)))
	if err != nil {
		return out, nil, err
	}

	switch cur[0].Kind {
	case lexer.KindTable:
		{
			ct, exp, err := parseCreateTable(expr)
			if err != nil {
				return out, nil, err
			}

			out.Type = CreateTypeTable
			out.CreateTable = ct
			expr = exp
		}
	case lexer.KindIndex:
		{
			ci, exp, err := parseCreateIndex(expr)
			if err != nil {
				return Create{}, nil, err
			}
			out.Type = CreateTypeIndex
			out.CreateIndex = ci
			expr = exp
		}
	}

	return out, expr, nil
}

func parseCreateTable(in *expr) (CreateTable, *expr, error) {
	cur, expr, err := in.read(is(lexer.KindIdentifier))
	if err != nil {
		return CreateTable{}, nil, err
	}

	name, ok := cur[0].Value.(string)
	if !ok {
		return CreateTable{}, nil, fmt.Errorf("invalid table name %v", cur[0].Value)
	}

	cols, expr, err := parseKeyValuePairs(expr)
	if err != nil {
		return CreateTable{}, nil, err
	}
	return CreateTable{
		Name:    object.Table(name),
		Columns: cols,
	}, expr, nil
}

func parseCreateIndex(in *expr) (CreateIndex, *expr, error) {
	cur, expr, err := in.read(is(lexer.KindIdentifier), is(lexer.KindOn), is(lexer.KindIdentifier), is(lexer.KindOpenParen))
	if err != nil {
		return CreateIndex{}, nil, err
	}

	fields, expr, err := parseFields(expr)
	if err != nil {
		return CreateIndex{}, nil, err
	}

	_, expr, err = expr.read(is(lexer.KindCloseParen))
	if err != nil {
		return CreateIndex{}, nil, err
	}

	return CreateIndex{
		Name:   cur[0].Value.(string),
		Table:  object.Table(cur[2].Value.(string)),
		Fields: fields,
	}, expr, nil
}

func parseInsert(in *expr) (Insert, *expr, error) {
	cur, expr, err := in.read(is(lexer.KindInto), is(lexer.KindIdentifier))
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

	_, expr, err = expr.read(is(lexer.KindValues))
	if err != nil {
		return Insert{}, nil, err
	}
	values, expr, err := parseValues(expr, cols)
	if err != nil {
		return Insert{}, nil, fmt.Errorf("parse insert values: %w", err)
	}

	return Insert{
		Table: object.Table(table),
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

func parseCSV(in *expr) ([]any, *expr, error) {
	_, expr, err := in.read(is(lexer.KindOpenParen))
	if err != nil {
		return nil, nil, err
	}
	var row []any
	for {
		cur, exp, err := expr.read(
			oneOf(is(lexer.KindNumberLiteral), is(lexer.KindStringLiteral), is(lexer.KindIdentifier)),
			oneOf(is(lexer.KindCloseParen), is(lexer.KindComma)),
		)
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

func parseKeyValuePairs(in *expr) ([]schema.Column, *expr, error) {
	_, expr, err := in.read(is(lexer.KindOpenParen))
	if err != nil {
		return nil, nil, err
	}
	var out []schema.Column
	for {
		cur, exp, err := expr.read(
			is(lexer.KindIdentifier),
			oneOf(is(lexer.KindText), is(lexer.KindNumber)),
			oneOf(is(lexer.KindCloseParen), is(lexer.KindComma)),
		)
		if err != nil {
			return nil, nil, err
		}

		expr = exp
		propName, ok := cur[0].Value.(string)
		if !ok {
			return nil, nil, fmt.Errorf("invalid property name %v", cur[0].Value)
		}
		prop := schema.Column{
			Name: propName,
		}
		if cur[1].Kind == lexer.KindText {
			prop.Type = schema.ColumnTypeText
		} else if cur[1].Kind == lexer.KindNumber {
			prop.Type = schema.ColumnTypeNumber
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

	cur, exp, err := expr.read(oneOf(
		is(lexer.KindComma), is(lexer.KindCloseParen),
	))
	if err != nil {
		if errors.Is(err, io.EOF) {
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

	joins, expr, err := parseJoins(expr)
	if err != nil {
		return Select{}, nil, err
	}

	where, expr, err := parseWhere(expr)
	if err != nil {
		return Select{}, nil, err
	}

	limit, expr, err := parseLimit(expr)
	if err != nil {
		return Select{}, nil, err
	}

	return Select{
		Fields:  fields,
		From:    from,
		Joins:   joins,
		Filters: where,
		Limit:   limit,
	}, expr, nil
}

func parseLimit(in *expr) (Limit, *expr, error) {
	r, expr, err := in.read(is(lexer.KindLimit), is(lexer.KindNumberLiteral))
	if err != nil {
		if errors.Is(io.EOF, err) {
			return Limit{}, in, nil
		}
		return Limit{}, nil, err
	}

	limit, ok := r[1].Value.(int32)
	if !ok {
		return Limit{}, in, errors.New("failed to parse limit")
	}
	return Limit{Limit: &limit}, expr, nil
}

func parseFields(in *expr) ([]Field, *expr, error) {
	var fields []Field
	expr := in

	for {
		field, exp, err := parseField(expr)
		if err != nil {
			return nil, nil, err
		}
		fields = append(fields, field)
		expr = exp

		_, exp, err = expr.read(
			is(lexer.KindComma),
		)
		if err != nil {
			break
		}

		expr = exp
	}

	return fields, expr, nil
}

func parseValue(in *expr) (Value, *expr, error) {
	if lit, expr, err := parseLiteral(in); err == nil {
		return Value{
			Type:  ValueTypeLitteral,
			Value: lit,
		}, expr, nil
	}

	if field, expr, err := parseField(in); err == nil {
		return Value{
			Type:      ValueTypeReference,
			Reference: field,
		}, expr, nil
	}

	if csv, expr, err := parseCSV(in); err == nil {
		return Value{
			Type:  ValueTypeList,
			Value: csv,
		}, expr, nil
	}

	return Value{}, nil, newUnexpectedTokenError(in.tokens[0])
}

func parseLiteral(in *expr) (any, *expr, error) {
	cur, expr, err := in.read(oneOf(
		is(lexer.KindStringLiteral),
		is(lexer.KindNumberLiteral),
	))
	if err != nil {
		return nil, nil, err
	}

	return cur[0].Value, expr, err
}

func parseField(in *expr) (Field, *expr, error) {
	cur, expr, err := in.read(is(lexer.KindIdentifier))
	if err != nil {
		return Field{}, nil, err
	}

	next, exp, err := expr.read(is(lexer.KindDot), is(lexer.KindIdentifier))
	if err != nil {
		return Field{
			Column: cur[0].Value.(string),
		}, expr, nil
	}

	return Field{
		Table:  object.Table(cur[0].Value.(string)),
		Column: next[1].Value.(string),
	}, exp, nil
}

func parseFrom(in *expr) (object.Table, *expr, error) {
	_, expr, err := in.read(is(lexer.KindFrom))
	if err != nil {
		return "", nil, err
	}

	cur, expr, err := expr.read(is(lexer.KindIdentifier))
	if err != nil {
		return "", nil, err
	}

	table, ok := cur[0].Value.(string)
	if !ok {
		return "", nil, fmt.Errorf("invalid table name %b", cur[0].Value)
	}

	return object.Table(table), expr, nil
}

func parseJoins(in *expr) ([]Join, *expr, error) {
	expr := in
	var joins []Join
	for {
		j, exp, err := parseJoin(expr)
		if err != nil {
			return nil, nil, err
		}
		if j == nil {
			break
		}
		expr = exp
		joins = append(joins, *j)
	}
	return joins, expr, nil
}

func parseJoin(in *expr) (*Join, *expr, error) {
	_, expr, err := in.read(is(lexer.KindJoin))
	if err != nil {
		return nil, in, nil
	}

	cur, expr, err := expr.read(
		is(lexer.KindIdentifier),
		is(lexer.KindOn),
	)

	table := object.Table(cur[0].Value.(string))

	filter, expr, err := parseFilter(expr)
	if err != nil {
		return nil, nil, err
	}

	var on On

	switch {
	case filter.Left.Reference.Table == table:
		on.Foreign = filter.Left.Reference
		on.Local = filter.Right.Reference
	case filter.Right.Reference.Table == table:
		on.Foreign = filter.Right.Reference
		on.Local = filter.Left.Reference
	default:
		return nil, nil, errors.New("join ON should always reference the joined table")
	}

	return &Join{
		Table: table,
		On:    on,
	}, expr, nil
}

func parseWhere(in *expr) ([]Filter, *expr, error) {
	_, expr, err := in.read(is(lexer.KindWhere))
	if err != nil {
		return nil, in, nil
	}

	return parseFilters(expr)
}

func parseFilters(in *expr) ([]Filter, *expr, error) {
	filter, expr, err := parseFilter(in)
	if err != nil {
		return nil, nil, err
	}
	var filters []Filter
	filters = append(filters, filter)
	for {
		_, exp, err := expr.read(is(lexer.KindAnd))
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

	return filters, expr, nil
}

func parseFilter(in *expr) (Filter, *expr, error) {
	left, expr, err := parseValue(in)
	if err != nil {
		return Filter{}, nil, err
	}

	cur, expr, err := expr.read(
		oneOf(
			is(lexer.KindEqual),
			is(lexer.KindAbove),
			is(lexer.KindBelow),
			is(lexer.KindIn),
		),
	)
	if err != nil {
		return Filter{}, nil, err
	}

	var op db.Op
	switch cur[0].Kind {
	case lexer.KindEqual:
		op = db.OpEqual
	case lexer.KindAbove:
		op = db.OpMoreThan
	case lexer.KindBelow:
		op = db.OpLessThan
	case lexer.KindIn:
		op = db.OpInclude
	}

	right, expr, err := parseValue(expr)
	if err != nil {
		return Filter{}, nil, err
	}

	return Filter{
		Left:  left,
		Op:    op,
		Right: right,
	}, expr, nil
}
