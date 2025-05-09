package eval

import (
	"context"
	"fmt"
	"slices"
	"strconv"

	"maps"

	"github.com/aliphe/filadb/db"
	"github.com/aliphe/filadb/db/index"
	"github.com/aliphe/filadb/db/object"
	"github.com/aliphe/filadb/db/schema"
	"github.com/aliphe/filadb/db/system"
	"github.com/aliphe/filadb/query/sql/parser"
	"github.com/google/uuid"
)

type Evaluator struct {
	client *db.Client
	shape  *system.DatabaseShape
}

func New(client *db.Client, shape *system.DatabaseShape) *Evaluator {
	return &Evaluator{
		client: client,
		shape:  shape,
	}
}

func (e *Evaluator) EvalExpr(ctx context.Context, q *parser.SQLQuery) ([]byte, error) {
	switch q.Type {
	case parser.QueryTypeInsert:
		n, err := e.evalInsert(ctx, q.Insert)
		if err != nil {
			return nil, err
		}
		return []byte("INSERT " + strconv.Itoa(n)), nil
	case parser.QueryTypeSelect:
		res, err := e.evalSelect(ctx, q.Select)
		if err != nil {
			return nil, err
		}
		return res, nil
	case parser.QueryTypeUpdate:
		n, err := e.evalUpdate(ctx, q.Update)
		if err != nil {
			return nil, err
		}
		return []byte("UPDATE " + strconv.Itoa(n)), nil
	case parser.QueryTypeCreate:
		switch q.Create.Type {
		case parser.CreateTypeIndex:
			return []byte("CREATE INDEX"), e.evalCreateIndex(ctx, q.Create.CreateIndex)
		case parser.CreateTypeTable:
			return []byte("CREATE TABLE"), e.evalCreateTable(ctx, q.Create.CreateTable)
		default:
			return nil, fmt.Errorf("unknown create type: %v", q.Create.Type)
		}
	default:
		return nil, fmt.Errorf("%s not implemented", q.Type)
	}
}
func (e *Evaluator) evalUpdate(ctx context.Context, update parser.Update) (int, error) {
	rows, err := e.scan(ctx, update.From, update.Filters...)
	if err != nil {
		return 0, fmt.Errorf("eval from: %w", err)
	}

	rows = unprefix(rows)

	for i, r := range rows {
		maps.Copy(r, update.Set.Update)
		if err := e.client.UpdateRow(ctx, update.From, r); err != nil {
			return i, fmt.Errorf("apply update for row %v: %w", r["id"], err)
		}
	}

	return len(rows), nil
}

func (e *Evaluator) evalCreateTable(ctx context.Context, create parser.CreateTable) error {
	sch := schema.Schema{
		Table:   create.Name,
		Columns: create.Columns,
	}

	return e.client.CreateSchema(ctx, &sch)
}

func (e *Evaluator) evalCreateIndex(ctx context.Context, create parser.CreateIndex) error {
	cols := make([]string, 0, len(create.Fields))
	for _, f := range create.Fields {
		cols = append(cols, f.Column)
	}
	idx := index.Index{
		Table:   create.Table,
		Name:    create.Name,
		Columns: cols,
	}
	return e.client.CreateIndex(ctx, &idx)
}

func (e *Evaluator) joinScan(ctx context.Context, cache []object.Row, j parser.Join) ([]object.Row, error) {
	filter := parser.Filter{
		Left: parser.Value{
			Type:      parser.ValueTypeReference,
			Reference: j.On.Foreign,
		},
	}
	cols := make([]any, 0, len(cache))
	for _, r := range cache {
		cols = append(cols, r[object.Key(j.On.Local.Table, j.On.Local.Column)])
	}

	filter.Op = db.OpInclude
	filter.Right = parser.Value{
		Value: cols,
		Type:  parser.ValueTypeList,
	}

	rows, err := e.scan(ctx, j.Table, filter)
	if err != nil {
		return nil, fmt.Errorf("join %s table: %w", j.Table, err)
	}

	return rows, nil
}

func (e *Evaluator) evalJoin(ctx context.Context, cache []object.Row, j parser.Join) ([]object.Row, error) {
	rows, err := e.joinScan(ctx, cache, j)
	if err != nil {
		return nil, err
	}

	byCol := make(map[any][]object.Row, len(rows))
	for _, r := range rows {
		k := object.Key(j.On.Foreign.Table, j.On.Foreign.Column)
		byCol[r[k]] = append(byCol[r[k]], r)
	}

	for _, r := range cache {
		k := object.Key(j.On.Local.Table, j.On.Local.Column)
		col := r[k]
		joined, ok := byCol[col]
		if ok {
			maps.Copy(r, joined[0])
			for i := range joined[1:] {
				joinedR := maps.Clone(r)
				maps.Copy(joinedR, joined[i+1])
				cache = append(cache, joinedR)
			}
		}
	}

	return cache, nil
}

func (e *Evaluator) outputCols(fields []parser.Field) []parser.Field {
	out := make([]parser.Field, 0, len(fields))
	for _, f := range fields {
		if f.Column == "*" {
			var tables []object.Table
			if f.Table == "" {
				for t := range e.shape.Schemas {
					tables = append(tables, t)
				}
			} else {
				tables = append(tables, f.Table)
			}
			for _, t := range tables {
				for _, c := range e.shape.Schemas[t].Columns {
					out = append(out, parser.Field{
						Table:  t,
						Column: c.Name,
					})
				}
			}
		} else {
			out = append(out, f)
		}
	}

	return out
}

func (e *Evaluator) formatRows(rows []object.Row, fields []parser.Field) []byte {
	fields = e.outputCols(fields)
	var out string
	for i, f := range fields {
		out += f.Column
		if i < len(fields)-1 {
			out += ","
		}
	}
	out += "\n"
	for i, row := range rows {
		for i, f := range fields {
			out += fmt.Sprint(row[e.key(f.Table, f.Column)])
			if i < len(fields)-1 {
				out += ","
			}
		}
		if i < len(rows)-1 {
			out += "\n"
		}
	}

	return []byte(out)
}

func (e *Evaluator) evalSelect(ctx context.Context, sel parser.Select) ([]byte, error) {
	from, err := e.scan(ctx, sel.From, sel.Filters...)
	if err != nil {
		return nil, fmt.Errorf("eval from: %w", err)
	}

	for _, j := range sel.Joins {
		res, err := e.evalJoin(ctx, from, j)
		if err != nil {
			return nil, err
		}
		from = res
	}

	return e.formatRows(from, sel.Fields), nil
}

func (e *Evaluator) key(table object.Table, col string) string {
	if table == "" {
		t := e.shape.ColMappings[col][0]
		return object.Key(t, col)
	}
	return object.Key(table, col)
}

func (e *Evaluator) evalInsert(ctx context.Context, ins parser.Insert) (int, error) {
	for i, r := range ins.Rows {
		if _, ok := r["id"]; !ok {
			r["id"] = uuid.New().String()
		}

		err := e.client.InsertRow(ctx, ins.Table, r)
		if err != nil {
			return i, err
		}
	}
	return len(ins.Rows), nil
}

func (e *Evaluator) scan(ctx context.Context, table object.Table, filters ...parser.Filter) ([]object.Row, error) {
	f := make([]db.Filter, 0, len(filters))
	for _, filter := range filters {
		if filter.Left.Reference.Table == table {
			f = append(f, db.Filter{
				Col: filter.Left.Reference.Column,
				Op:  filter.Op,
				Val: filter.Right.Value,
			})
		}
	}
	var rows []object.Row
	err := e.client.Scan(ctx, table, &rows, f...)
	if err != nil {
		return nil, err
	}

	return e.filter(prefix(table, rows), filters), nil
}

// prefix adds table prefix to all columns in object
func prefix(table object.Table, rows []object.Row) []object.Row {
	out := make([]object.Row, 0, len(rows))
	for _, r := range rows {
		new := make(object.Row)
		for k, v := range r {
			new[object.Key(table, k)] = v
		}
		out = append(out, new)
	}

	return out
}

func unprefix(rows []object.Row) []object.Row {
	out := make([]object.Row, 0, len(rows))
	for _, r := range rows {
		new := make(object.Row)
		for k, v := range r {
			new[object.ParseCol(k)] = v
		}
		out = append(out, new)
	}

	return out
}

func (e *Evaluator) filter(rows []object.Row, f []parser.Filter) []object.Row {
	var out []object.Row
	for _, r := range rows {
		if e.matches(r, f) {
			out = append(out, r)
		}
	}

	return out
}

func (e *Evaluator) matches(row object.Row, filters []parser.Filter) bool {
	for _, f := range filters {
		var ref parser.Field
		var val any
		if f.Left.Type == parser.ValueTypeReference {
			ref = f.Left.Reference
			val = f.Right.Value
		} else {
			val = f.Left.Value
			ref = f.Right.Reference
		}
		lk := e.key(ref.Table, ref.Column)
		switch f.Op {
		case db.OpEqual:
			return row[lk] == val
		case db.OpInclude:
			vals, ok := val.([]any)
			if !ok {
				return false
			}
			return slices.Contains(vals, row[lk])
		}
	}

	return true
}
