package schema

import (
	"fmt"

	"github.com/linkedin/goavro/v2"
)

type Validator struct {
	codec *goavro.Codec
}

func (v *Validator) Marshall(object interface{}) ([]byte, error) {
	b, err := v.codec.BinaryFromNative(nil, object)
	if err != nil {
		return nil, fmt.Errorf("invalid format: %w", err)
	}

	return b, nil
}

func (v *Validator) Unmarshall(object []byte) (interface{}, error) {
	out, _, err := v.codec.NativeFromBinary(object)
	if err != nil {
		return nil, fmt.Errorf("invalid format: %w", err)
	}

	return out, nil
}
