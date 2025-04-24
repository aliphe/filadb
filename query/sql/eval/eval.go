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
	"github.com/aliphe/filadb/query/sql/parser"
	"github.com/google/uuid"
)

/*
select * from users
inner join posts on posts.user_id = users.id
inner join comments on posts.id = comments.post_id

users:

	1->id:1,name:alif
	2->id:2,name:alof

posts:

	1->id:3,title:post 3
	 ->id:4,title:post 4

comments:

	4->id:5,content:super
*/
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
	rows, err := e.scan(ctx, update.From, update.Filters...)
	if err != nil {
		return 0, fmt.Errorf("eval from: %w", err)
	}

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

func extractCols(cache map[any]object.Row, col string) ([]any, error) {
	cols := make([]any, 0, len(cache))
	for _, r := range cache {
		cols = append(cols, r[col])
	}

	return cols, nil
}

func (e *Evaluator) evalJoin(ctx context.Context, res map[any]object.Row, j parser.Join) error {
	filter := parser.Filter{
		Left: parser.Value{
			Type: parser.ValueTypeReference,
			Reference: parser.Field{
				Table:  j.Table,
				Column: j.On.Foreign,
			},
		},
	}
	cols, err := extractCols(res, j.On.Local)
	if err != nil {
		return err
	}
	filter.Op = parser.OpInclude
	filter.Right = parser.Value{
		Value: cols,
		Type:  parser.ValueTypeList,
	}

	rows, err := e.scan(ctx, j.Table, filter)
	if err != nil {
		return fmt.Errorf("eval from: %w", err)
	}

	byCol := make(map[any][]object.Row, len(rows))
	for _, r := range rows {
		byCol[r[j.On.Foreign]] = append(byCol[r[j.On.Foreign]], r)
	}

	toAdd := make(map[string]object.Row)
	for _, r := range res {
		if rr, ok := byCol[r[j.On.Local]]; ok {
			maps.Copy(r, rr[0])
			for i := range rr[1:] {
				r = maps.Clone(r)
				toAdd[string(r.ObjectID())] = r
				maps.Copy(r, rr[i])
			}
		}
	}

	return nil
}

func (e *Evaluator) evalSelect(ctx context.Context, sel parser.Select) ([]byte, error) {
	from, err := e.scan(ctx, sel.From, sel.Filters...)
	if err != nil {
		return nil, fmt.Errorf("eval from: %w", err)
	}

	cache := make(map[any]object.Row, len(from))
	for _, r := range from {
		cache[r.ObjectID()] = r
	}
	for _, j := range sel.Joins {
		if err := e.evalJoin(ctx, cache, j); err != nil {
			return nil, err
		}
	}

	fields := make([]string, 0, len(sel.Fields))

	sh, err := e.client.Shape(ctx, sel.From)
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

func (e *Evaluator) scan(ctx context.Context, table object.Table, filters ...parser.Filter) ([]object.Row, error) {
	f := make([]db.Filter, 0, len(filters))
	for _, filter := range filters {
		if filter.Left.Reference.Table == table {
			f = append(f, db.Filter{
				Col: filter.Left.Reference.Column,
				Val: filter.Right.Value,
			})
		}
	}
	var rows []object.Row
	err := e.client.Scan(ctx, table, &rows, f...)
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
		var ref parser.Field
		var val any
		if f.Left.Type == parser.ValueTypeReference {
			ref = f.Left.Reference
			val = f.Right.Value
		} else {
			val = f.Left.Value
			ref = f.Right.Reference
		}
		lk := key(ref)
		switch f.Op {
		case parser.OpEqual:
			return row[lk] == val
		case parser.OpInclude:
			vals, ok := val.([]any)
			if !ok {
				return false
			}
			return slices.Contains(vals, row[lk])
		}
	}

	return true
}
