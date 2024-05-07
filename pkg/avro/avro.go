package avro

import (
	_ "embed"
	"fmt"

	"github.com/linkedin/goavro/v2"
)

func Marshal(schema string, obj map[string]interface{}) ([]byte, error) {
	c, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, fmt.Errorf("invalid schema: %w", err)
	}

	b, err := c.BinaryFromNative(nil, obj)
	if err != nil {
		return nil, fmt.Errorf("invalid schema: %w", err)
	}

	return b, nil
}

func Unmarshal(schema string, b []byte) (map[string]interface{}, error) {
	c, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, fmt.Errorf("invalid schema: %w", err)
	}

	out, _, err := c.NativeFromBinary(b)
	if err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}

	return out.(map[string]interface{}), nil
}
