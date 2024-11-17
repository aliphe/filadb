package object

type Marshaler interface {
	Shape() []string
	Marshal(obj interface{}) ([]byte, error)
	Unmarshal(b []byte, dst interface{}) error
	UnmarshalBatch(b [][]byte, dst interface{}) error
}
