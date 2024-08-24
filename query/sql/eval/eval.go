package eval

import (
	"context"
	"fmt"

	"github.com/aliphe/filadb/db"
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

func (e *Evaluator) EvalExpr(ctx context.Context, ast parser.SQLQuery) ([]object.Row, error) {
	if ast.Type == parser.QueryTypeInsert {
		return nil, e.evalInsert(ctx, ast.Insert)
	} else if ast.Type == parser.QueryTypeSelect {
		return e.evalSelect(ctx, ast.Select)
	} else if ast.Type == parser.QueryTypeUpdate {
		return nil, e.evalUpdate(ctx, ast.Update)
	} else if ast.Type == parser.QueryTypeCreateTable {
		return nil, e.evalCreateTable(ctx, ast.CreateTable)
	} else {
		return nil, fmt.Errorf("%s not implemented", ast.Type)
	}
}

func (e *Evaluator) evalUpdate(ctx context.Context, update parser.Update) error {
	rows, err := e.evalFrom(ctx, update.From)
	if err != nil {
		return fmt.Errorf("eval from: %w", err)
	}
	q, err := e.client.Acquire(ctx, update.From.Table)
	if err != nil {
		return err
	}

	for _, r := range rows {
		for k, v := range update.Set.Update {
			r[k] = v
		}
		if err := q.Update(ctx, r); err != nil {
			return fmt.Errorf("apply update for row %v: %w", r["id"], err)
		}
	}

	return nil
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

	var all bool
	for _, s := range sel.Fields {
		if s.Column == "*" {
			all = true
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

func (e *Evaluator) evalInsert(ctx context.Context, ins parser.Insert) error {
	q, err := e.client.Acquire(ctx, ins.Table)
	if err != nil {
		return err
	}
	for _, r := range ins.Rows {
		if _, ok := r["id"]; !ok {
			r["id"] = uuid.New().String()
		}

		err := q.Insert(ctx, r)
		if err != nil {
			return err
		}
	}
	return nil
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
