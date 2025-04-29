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

func extractCols(cache []object.Row, field parser.Field) ([]any, error) {
	cols := make([]any, 0, len(cache))
	for _, r := range cache {
		cols = append(cols, r[key(field.Table, field.Column)])
	}

	return cols, nil
}

func (e *Evaluator) evalJoin(ctx context.Context, res []object.Row, j parser.Join) error {
	filter := parser.Filter{
		Left: parser.Value{
			Type:      parser.ValueTypeReference,
			Reference: j.On.Foreign,
		},
	}
	cols, err := extractCols(res, j.On.Local)
	if err != nil {
		return err
	}
	filter.Op = db.OpInclude
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
		k := key(j.On.Foreign.Table, j.On.Foreign.Column)
		byCol[r[k]] = append(byCol[r[k]], r)
	}

	for _, r := range res {
		k := key(j.On.Local.Table, j.On.Local.Column)
		col := r[k]
		joined, ok := byCol[col]
		if ok {
			maps.Copy(r, joined[0])
			for i := range joined[1:] {
				joinedR := maps.Clone(r)
				maps.Copy(joinedR, joined[i])
				res = append(res, joinedR)
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

	for _, j := range sel.Joins {
		if err := e.evalJoin(ctx, from, j); err != nil {
			return nil, err
		}
	}

	fields := make([]parser.Field, 0, len(sel.Fields))

	for _, s := range sel.Fields {
		if s.Column != "*" {
			fields = append(fields, s)
		}
	}

	var out string
	for i, f := range fields {
		out += f.Column
		if i < len(fields)-1 {
			out += ","
		}
	}
	out += "\n"
	for _, row := range from {
		for i, f := range fields {
			out += fmt.Sprint(row[key(f.Table, f.Column)])
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

	// prefix cols with table name, like users.id
	for _, r := range rows {
		newValues := make(object.Row)
		for k, v := range r {
			newValues[key(table, k)] = v
		}
		maps.Copy(r, newValues)
		for k := range r {
			if _, isNewKey := newValues[k]; !isNewKey {
				delete(r, k)
			}
		}
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

func key(table object.Table, col string) string {
	if table != "" {
		return string(fmt.Sprintf("%s.%s", table, col))
	}

	return col
}

func matches(row object.Row, filters []parser.Filter) bool {
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
		lk := key(ref.Table, ref.Column)
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
