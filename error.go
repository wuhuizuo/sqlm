package sqlm

// ErrorSQLInvalid error when composed invalid sql statement
type ErrorSQLInvalid struct {
	Message string
	Err     error
}

// Error error message
func (e *ErrorSQLInvalid) Error() string {
	return e.Message
}

// Unwrap return source error
func (e *ErrorSQLInvalid) Unwrap() error {
	return e.Err
}
