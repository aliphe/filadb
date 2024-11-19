package schema

import (
	"bytes"
	"encoding/gob"
	"errors"
	"reflect"
)

type marshaler struct {
	src    *Schema
	schema string
}

func (a *marshaler) Shape() []string {
	out := make([]string, 0, len(a.src.Columns))
	for _, c := range a.src.Columns {
		out = append(out, c.Name)
	}

	return out
}

func (a *marshaler) Marshal(obj interface{}) ([]byte, error) {
	w := new(bytes.Buffer)
	enc := gob.NewEncoder(w)

	if err := enc.Encode(obj); err != nil {
		return nil, err
	}

	return w.Bytes(), nil
}

func (a *marshaler) Unmarshal(b []byte, dst interface{}) error {
	r := bytes.NewReader(b)
	dec := gob.NewDecoder(r)

	if err := dec.Decode(dst); err != nil {
		return err
	}

	return nil
}

func (a *marshaler) UnmarshalBatch(s [][]byte, dst interface{}) error {
	dstValue := reflect.ValueOf(dst)
	if dstValue.Kind() != reflect.Ptr || dstValue.Elem().Kind() != reflect.Slice {
		return errors.New("dst must be a pointer to a slice")
	}

	sliceValue := dstValue.Elem()
	elementType := sliceValue.Type().Elem()

	for _, r := range s {
		newElement := reflect.New(elementType).Interface()
		if err := a.Unmarshal(r, newElement); err != nil {
			return err
		}
		sliceValue = reflect.Append(sliceValue, reflect.ValueOf(newElement).Elem())
	}

	dstValue.Elem().Set(sliceValue)
	return nil
}
