package schema

type Schema struct {
	Table      string
	Properties []*Property
}

type Property struct {
	Name string
	Type PropertyType
}

type PropertyType string

const (
	PropertyTypeText     PropertyType = "text"
	PropertyTypeNumber   PropertyType = "number"
	PropertyTypeDateTime PropertyType = "datetime"
)

type InternalTable string

const (
	InternalTableSchemas InternalTable = "schemas"
)

type ReaderWriter struct {
	Reader
	Writer
}
