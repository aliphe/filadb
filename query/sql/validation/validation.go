package validation

import (
	"github.com/aliphe/filadb/db/object"
	"github.com/aliphe/filadb/db/system"
	"github.com/aliphe/filadb/query/sql/parser"
)

type SanityChecker struct {
	shape       system.DatabaseShape
	colMappings map[string][]object.Table
	allCols     map[string]bool
}

func NewSanityChecker(shape system.DatabaseShape) *SanityChecker {
	colMappings := make(map[string][]object.Table)
	allCols := make(map[string]bool)
	for t, sch := range shape {
		for _, c := range sch.Columns {
			colMappings[c.Name] = append(colMappings[c.Name], t)
			allCols[string(t)+c.Name] = true
		}
	}

	return &SanityChecker{
		shape:       shape,
		colMappings: colMappings,
		allCols:     allCols,
	}
}

func (sc *SanityChecker) Check(q *parser.SQLQuery) error {
	switch q.Type {
	case parser.QueryTypeSelect:
		{
			return sc.checkSelect(&q.Select)
		}
	}

	return nil
}

func (sc *SanityChecker) checkSelect(q *parser.Select) error {
	if err := sc.checkFields(q.Fields); err != nil {
		return err
	}

	return nil
}

func (sc *SanityChecker) checkFields(fields []parser.Field) error {
	for _, f := range fields {
		if f.Table == "" {
			tables := sc.colMappings[f.Column]
			if len(tables) == 0 {
				return ErrReferenceNotFound
			} else if len(tables) > 1 {
				return ErrAmbiguousReference
			}
		} else {
			if _, ok := sc.allCols[string(f.Table)+f.Column]; !ok {
				return ErrReferenceNotFound
			}
		}
	}

	return nil
}
