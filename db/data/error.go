package data

// Error represents a database specific error
type Error string

// Error implements built-in error interface
func (e Error) Error() string {
	return string(e)
}

const (
	ErrTableNotFound = Error("table not found")
)
