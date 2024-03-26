package query

import (
	_ "embed"
)

//go:embed sql.peg
var sqlGrammar string

func Parse(q string) error {
	return nil
}
