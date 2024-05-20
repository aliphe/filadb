package schema

import (
	"context"
	"errors"
	"fmt"

	"github.com/aliphe/filadb/btree"
	"github.com/aliphe/filadb/db/object"
	"github.com/aliphe/filadb/db/storage"
)

var (
	ErrTableNotFound = errors.New("schema not found")
)

type Admin struct {
	rw                 storage.ReaderWriter
	marshallers        map[string]*avroMarshaler
	marshallerVersions map[string]int32
}

func NewAdmin(rw storage.ReaderWriter) (*Admin, error) {
	a := &Admin{
		rw:                 rw,
		marshallers:        make(map[string]*avroMarshaler),
		marshallerVersions: make(map[string]int32),
	}
	return a, a.init()
}

func (a *Admin) init() error {
	_, ok, err := a.rw.Get(context.Background(), string(internalTableTables), string(internalTableTables))
	if !ok || errors.Is(err, btree.ErrNodeNotFound) {
		m := &avroMarshaler{toSchema(&internalTableTablesSchema)}

		if err := a.createTable(context.Background(), m, string(internalTableTables)); err != nil {
			return fmt.Errorf("create internal 'tables' table: %w", err)
		}
		if err := a.createTable(context.Background(), m, string(internalTableColumns)); err != nil {
			return fmt.Errorf("create internal 'columns' table: %w", err)
		}
		a.marshallers[string(internalTableTables)] = m
		a.marshallerVersions[string(internalTableTables)] = 1
	}

	b, err := a.rw.Scan(context.Background(), string(internalTableColumns))
	if len(b) == 0 || errors.Is(err, btree.ErrNodeNotFound) {
		m := &avroMarshaler{toSchema(&internalTableColumnsSchema)}
		if err := a.createColumns(context.Background(), m, &internalTableTablesSchema); err != nil {
			return fmt.Errorf("create internal 'tables' table columns: %w", err)
		}
		if err := a.createColumns(context.Background(), m, &internalTableColumnsSchema); err != nil {
			return fmt.Errorf("create internal 'columns' table columns: %w", err)
		}
		a.marshallers[string(internalTableColumns)] = m
		a.marshallerVersions[string(internalTableColumns)] = 1
	}
	return nil
}

func (a *Admin) marshaler(ctx context.Context, table string) (*avroMarshaler, error) {
	sch, err := fromStorage(ctx, a.rw, table)
	if err != nil {
		return nil, fmt.Errorf("acquire table marshaler: %w", err)
	}

	if sch.version == a.marshallerVersions[table] {
		return a.marshallers[table], nil
	}

	m, v, err := newMarshaler(ctx, a.rw, table)
	if err != nil {
		return nil, fmt.Errorf("acquire table marshaler: %w", err)
	}
	a.marshallers[table] = m
	a.marshallerVersions[table] = v
	return m, nil
}

func (a *Admin) Marshal(ctx context.Context, table string, obj object.Row) ([]byte, error) {
	m, err := a.marshaler(ctx, table)
	if err != nil {
		return nil, err
	}

	b, err := m.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("marshal data: %w", err)
	}

	return b, nil
}

func (a *Admin) Unmarshal(ctx context.Context, table string, b []byte) (object.Row, error) {
	m, err := a.marshaler(ctx, table)
	if err != nil {
		return nil, err
	}

	out, err := m.Unmarshal(b)
	if err != nil {
		return nil, fmt.Errorf("unmarshal data: %w", err)
	}

	return out, nil
}

func (a *Admin) UnmarshalBatch(ctx context.Context, table string, s [][]byte) ([]object.Row, error) {
	m, err := a.marshaler(ctx, table)
	if err != nil {
		return nil, err
	}

	out, err := m.UnmarshalBatch(s)
	if err != nil {
		return nil, fmt.Errorf("unmarshal data: %w", err)
	}

	return out, nil
}

func (a *Admin) Create(ctx context.Context, schema *Schema) error {
	m, err := a.marshaler(ctx, string(internalTableTables))
	if err != nil {
		return err
	}
	err = a.createTable(ctx, m, schema.Table)
	if err != nil {
		return err
	}

	m, err = a.marshaler(ctx, string(internalTableColumns))
	if err != nil {
		return err
	}
	err = a.createColumns(ctx, m, schema)

	return nil
}

func (a *Admin) createTable(ctx context.Context, m *avroMarshaler, table string) error {
	b, err := m.Marshal(object.Row{
		"table":   table,
		"version": 1,
	})
	if err != nil {
		return fmt.Errorf("encode schema: %w", err)
	}

	err = a.rw.Add(ctx, string(internalTableTables), table, b)
	if err != nil {
		return fmt.Errorf("save schema: %w", err)
	}

	return nil
}

func (a *Admin) createColumns(ctx context.Context, m *avroMarshaler, schema *Schema) error {
	for _, col := range schema.Columns {
		b, err := m.Marshal(object.Row{
			"table":  schema.Table,
			"column": col.Name,
			"type":   avroTypeMapper[col.Type],
		})
		if err != nil {
			return fmt.Errorf("marshal column %s: %w", col.Name, err)
		}
		err = a.rw.Add(ctx, string(internalTableColumns), schema.Table+"."+col.Name, b)
		if err != nil {
			return fmt.Errorf("save column %s: %w", col.Name, err)
		}
	}

	return nil
}
