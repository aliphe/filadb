package object

type Filter struct {
	Column string
	Op     Op
	Value  interface{}
}

type Op int

const (
	OpEqual = iota
)
