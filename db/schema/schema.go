package schema

type Schema struct {
	Table      string
	Properties []*Property
}

type Property struct {
	Name       string
	PrimaryKey bool
	Type       PropertyType
}

type PropertyType string

const (
	PropertyTypeText     PropertyType = "text"
	PropertyTypeNumber   PropertyType = "number"
	PropertyTypeDateTime PropertyType = "datetime"
)
