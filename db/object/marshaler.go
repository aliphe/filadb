package object

type Marshaler interface {
	Marshal(obj interface{}) ([]byte, error)
	Unmarshal(b []byte, dst interface{}) error
	UnmarshalBatch(b [][]byte, dst interface{}) error
}
