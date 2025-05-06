package validation

import (
	"fmt"

	"github.com/aliphe/filadb/db/object"
	"github.com/aliphe/filadb/db/system"
	"github.com/aliphe/filadb/query/sql/parser"
)

type SanityChecker struct {
	shape *system.DatabaseShape
}

func NewSanityChecker(shape *system.DatabaseShape) *SanityChecker {
	return &SanityChecker{
		shape: shape,
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
		if f.Column == "*" {
			continue
		}
		if f.Table == "" {
			tables := sc.shape.ColMappings[f.Column]
			if len(tables) == 0 {
				return fmt.Errorf("%s: %w", f.Column, ErrReferenceNotFound)
			} else if len(tables) > 1 {
				return fmt.Errorf("%s: %w", f.Column, ErrAmbiguousReference)
			}
		} else {
			if _, ok := sc.shape.AllCols[f.Table][f.Column]; !ok {
				return fmt.Errorf("%s: %w", object.Key(f.Table, f.Column), ErrReferenceNotFound)
			}
		}
	}

	return nil
}
