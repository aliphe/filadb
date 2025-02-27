package eval

import (
	"context"
	"fmt"
	"strconv"

	"github.com/aliphe/filadb/db"
	"github.com/aliphe/filadb/db/index"
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
			return res, nil
		}
	case parser.QueryTypeUpdate:
		{
			n, err := e.evalUpdate(ctx, ast.Update)
			if err != nil {
				return nil, err
			}
			return raw("UPDATE " + strconv.Itoa(n)), nil
		}
	case parser.QueryTypeCreate:
		{
			if ast.Create.Type == parser.CreateTypeIndex {
				return raw("CREATE INDEX"), e.evalCreateIndex(ctx, ast.Create.CreateIndex)
			} else if ast.Create.Type == parser.CreateTypeTable {
				return raw("CREATE TABLE"), e.evalCreateTable(ctx, ast.Create.CreateTable)
			}
			return nil, nil
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

	for i, r := range rows {
		for k, v := range update.Set.Update {
			r[k] = v
		}
		if err := e.client.UpdateRow(ctx, update.From.Table, r); err != nil {
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

func (e *Evaluator) evalSelect(ctx context.Context, sel parser.Select) ([]byte, error) {
	from, err := e.evalFrom(ctx, sel.From)
	if err != nil {
		return nil, fmt.Errorf("eval from: %w", err)
	}

	fields := make([]string, 0, len(sel.Fields))

	sh, err := e.client.Shape(ctx, sel.From.Table)
	if err != nil {
		return nil, err
	}

	for _, s := range sel.Fields {
		if s.Column == "*" {
			fields = append(fields, sh...)
		} else {
			fields = append(fields, s.Column)
		}
	}

	var out string
	for i, f := range fields {
		out += f
		if i < len(fields)-1 {
			out += ","
		}
	}
	out += "\n"
	for _, row := range from {
		for i, f := range fields {
			out += fmt.Sprint(row[f])
			if i < len(fields)-1 {
				out += ","
			}
		}
		out += "\n"
	}

	return []byte(out), nil
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

func (e *Evaluator) evalFrom(ctx context.Context, from parser.From) ([]object.Row, error) {
	var filters []parser.Filter
	if from.Where != nil {
		filters = from.Where
	}
	return e.scan(ctx, from.Table, filters...)
}

func (e *Evaluator) scan(ctx context.Context, table object.Table, filters ...parser.Filter) ([]object.Row, error) {
	var rows []object.Row
	err := e.client.Scan(ctx, table, &rows, filters...)
	if err != nil {
		return nil, err
	}

	return filter(rows, filters), nil
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
	key := func(v parser.Field) string {
		if v.Table != "" {
			return string(v.Table) + "." + v.Column
		}
		return v.Column
	}

	for _, f := range filters {
		lk := key(f.Left.Reference)
		if f.Op == parser.OpEqual && row[lk] != f.Right.Value {
			return false
		}
	}

	return true
}
