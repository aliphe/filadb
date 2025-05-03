package validation

import "errors"

var (
	ErrReferenceNotFound  = errors.New("reference not found")
	ErrAmbiguousReference = errors.New("ambiguous reference")
)
