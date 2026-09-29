package gateway

import "errors"

type UncertainError struct{ err error }

func (e *UncertainError) Error() string { return "uncertain result: " + e.err.Error() }
func (e *UncertainError) Unwrap() error { return e.err }

func Uncertain(err error) error {
	if err == nil {
		return nil
	}
	return &UncertainError{err: err}
}

func IsUncertain(err error) bool {
	var target *UncertainError
	return errors.As(err, &target)
}
