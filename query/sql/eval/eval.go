package eval

import (
	"bytes"
	"context"
	"fmt"
	"strconv"

	"github.com/aliphe/filadb/db"
	"github.com/aliphe/filadb/db/csv"
	"github.com/aliphe/filadb/db/object"
	"github.com/aliphe/filadb/db/schema"
	"github.com/aliphe/filadb/query/sql/parser"
	"github.com/google/uuid"
)

type Evaluator struct {
	client *db.Client
}

func New(client *db.Client) *Evaluator {
	return &Evaluator{
		client: client,
	}
}

func toCsv(rows []object.Row) ([]byte, error) {
	var b bytes.Buffer
	csv := csv.NewWriter(&b)
	err := csv.Write(rows)
	if err != nil {
		return []byte(fmt.Sprintf("marshall result: %s", err)), nil
	}
	return b.Bytes(), nil
}

func raw(s string) []byte {
	return []byte(s + "\n")
}

func (e *Evaluator) EvalExpr(ctx context.Context, ast parser.SQLQuery) ([]byte, error) {
	switch ast.Type {
	case parser.QueryTypeInsert:
		{
			n, err := e.evalInsert(ctx, ast.Insert)
			if err != nil {
				return nil, err
			}
			return raw("INSERT " + strconv.Itoa(n)), nil
		}
	case parser.QueryTypeSelect:
		{
			res, err := e.evalSelect(ctx, ast.Select)
			if err != nil {
				return nil, err
			}
			return toCsv(res)
		}
	case parser.QueryTypeUpdate:
		{
			n, err := e.evalUpdate(ctx, ast.Update)
			if err != nil {
				return nil, err
			}
			return raw("UPDATE " + strconv.Itoa(n)), nil
		}
	case parser.QueryTypeCreateTable:
		{
			return raw("CREATE TABLE"), e.evalCreateTable(ctx, ast.CreateTable)
		}
	default:
		{
			return nil, fmt.Errorf("%s not implemented", ast.Type)
		}
	}
}

func (e *Evaluator) evalUpdate(ctx context.Context, update parser.Update) (int, error) {
	rows, err := e.evalFrom(ctx, update.From)
	if err != nil {
		return 0, fmt.Errorf("eval from: %w", err)
	}
	q, err := e.client.Acquire(ctx, update.From.Table)
	if err != nil {
		return 0, err
	}

	for i, r := range rows {
		for k, v := range update.Set.Update {
			r[k] = v
		}
		if err := q.Update(ctx, r); err != nil {
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

func (e *Evaluator) evalSelect(ctx context.Context, sel parser.Select) ([]object.Row, error) {
	from, err := e.evalFrom(ctx, sel.From)
	if err != nil {
		return nil, fmt.Errorf("eval from: %w", err)
	}

	fields := make([]parser.Field, 0, len(sel.Fields))

	// not fan of this
	sh, err := e.client.Shape(ctx, sel.From.Table)
	if err != nil {
		return nil, err
	}

	for _, s := range sel.Fields {
		if s.Column == "*" {
			// add all fields in sh
		} else {
			fields = append(fields, s)
		}
	}

	if all {
		return from, nil
	}

	out := make([]object.Row, 0, len(from))
	for _, row := range from {
		ins := make(object.Row)
		for _, f := range sel.Fields {
			ins[f.Column] = row[f.Column]
		}
		out = append(out, ins)
	}
	return out, nil
}

func (e *Evaluator) evalInsert(ctx context.Context, ins parser.Insert) (int, error) {
	q, err := e.client.Acquire(ctx, ins.Table)
	if err != nil {
		return 0, err
	}
	for i, r := range ins.Rows {
		if _, ok := r["id"]; !ok {
			r["id"] = uuid.New().String()
		}

		err := q.Insert(ctx, r)
		if err != nil {
			return i, err
		}
	}
	return len(ins.Rows), nil
}

func (e *Evaluator) evalFrom(ctx context.Context, from parser.From) ([]object.Row, error) {
	q, err := e.client.Acquire(ctx, from.Table)
	if err != nil {
		return nil, err
	}
	id, hasId := idFilter(from.Where)
	if hasId {
		var row object.Row
		ok, err := q.Get(ctx, id, &row)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, nil
		}
		return []object.Row{row}, nil
	}

	var rows []object.Row
	err = q.Scan(ctx, &rows)
	if err != nil {
		return nil, err
	}

	if from.Where != nil {
		return filter(rows, from.Where.Filters), nil
	}

	return rows, nil
}

func idFilter(where *parser.Where) (object.ID, bool) {
	if where != nil {
		for _, f := range where.Filters {
			if f.Column == "id" && f.Op == parser.OpEqual {
				v, ok := f.Value.(string)
				if ok {
					return object.ID(v), true
				}
			}
		}
	}
	return "", false
}

func filter(rows []object.Row, f []parser.Filter) []object.Row {
	var out []object.Row
	for _, r := range rows {
		if matches(r, f) {
			out = append(out, r)
		}
	}

	return out
}

func matches(row object.Row, filters []parser.Filter) bool {
	for _, f := range filters {
		if f.Op == parser.OpEqual && row[f.Column] != f.Value {
			return false
		}
	}

	return true
}
