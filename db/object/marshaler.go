package object

type Marshaler interface {
	Shape() []string
	Marshal(obj any) ([]byte, error)
	Unmarshal(b []byte, dst any) error
	UnmarshalBatch(b [][]byte, dst any) error
}
