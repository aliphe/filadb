package parser

type Expr struct {
	Select
}

type Select struct {
	Fields  []Field
	Sources []Source
}

type Field struct {
	Source string
	Col    string
}

type Source struct {
	Name   string
	Filter Filter
}

type Filter func(interface{}) bool
