// Package validation provides errors for domain validation failures.
package validation

import "fmt"

// Error identifies invalid user-provided domain data.
type Error struct {
	err error
}

// Errorf formats a validation error.
func Errorf(format string, args ...any) *Error {
	return &Error{err: fmt.Errorf(format, args...)}
}

func (e *Error) Error() string {
	return e.err.Error()
}

func (e *Error) Unwrap() error {
	return e.err
}
