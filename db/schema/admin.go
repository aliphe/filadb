package schema

import (
	"context"
	"errors"
	"fmt"

	"github.com/aliphe/filadb/btree"
	"github.com/aliphe/filadb/db/object"
	"github.com/aliphe/filadb/db/storage"
	"github.com/aliphe/filadb/pkg/avro"
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
	if err := initdb(rw); err != nil {
		return nil, err
	}
	return &Admin{
		rw:                 rw,
		marshallers:        make(map[string]*avroMarshaler),
		marshallerVersions: make(map[string]int32),
	}, nil
}

func initdb(rw storage.ReaderWriter) error {
	_, ok, err := rw.Get(context.Background(), string(InternalTableTables), string(InternalTableTables))
	if !ok || errors.Is(err, btree.ErrNodeNotFound) {
		b, err := avro.Marshal(toSchema(&internalTableTablesSchema), object.Row{
			"table":   "tables",
			"version": 1,
		})
		if err != nil {
			return err
		}
		err = rw.Add(context.Background(), string(InternalTableTables), string(InternalTableTables), b)
		if err != nil {
			return err
		}
	}

	_, ok, err = rw.Get(context.Background(), string(InternalTableTables), string(InternalTableColumns))
	if !ok || errors.Is(err, btree.ErrNodeNotFound) {
		b, err := avro.Marshal(toSchema(&internalTableTablesSchema), object.Row{
			"table":   "columns",
			"version": 1,
		})
		if err != nil {
			return err
		}
		err = rw.Add(context.Background(), string(InternalTableTables), string(InternalTableColumns), b)
		if err != nil {
			return err
		}
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
	m, err := a.marshaler(ctx, string(InternalTableTables))
	if err != nil {
		return err
	}
	b, err := m.Marshal(object.Row{
		"table":   schema.Table,
		"version": "1",
	})
	if err != nil {
		return fmt.Errorf("encode schema: %w", err)
	}

	err = a.rw.Add(ctx, string(InternalTableTables), string(InternalTableTables), b)
	if err != nil {
		return fmt.Errorf("save schema: %w", err)
	}

	m, err = a.marshaler(ctx, string(InternalTableColumns))
	if err != nil {
		return err
	}
	for _, col := range schema.Columns {
		b, err := m.Marshal(object.Row{
			"table":  schema.Table,
			"column": col.Name,
			"type":   avroTypeMapper[col.Type],
		})
		if err != nil {
			return fmt.Errorf("marshal column %s: %w", col.Name, err)
		}
		err = a.rw.Add(ctx, string(InternalTableColumns), col.Name, b)
		if err != nil {
			return fmt.Errorf("marshal column %s: %w", col.Name, err)
		}
	}

	return nil
}
