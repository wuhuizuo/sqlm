package sqlm

import (
	"errors"
	"testing"
)

func TestErrorSQLInvalid_Error(t *testing.T) {
	tests := []struct {
		name string
		err  *ErrorSQLInvalid
		want string
	}{
		{"empty", &ErrorSQLInvalid{}, ""},
		{"filled", &ErrorSQLInvalid{Message: "error"}, "error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.Error(); got != tt.want {
				t.Errorf("ErrorSQLInvalid.Error() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestErrorSQLInvalid_Unwrap(t *testing.T) {
	tests := []struct {
		name    string
		err     *ErrorSQLInvalid
		wantErr bool
	}{
		{"empty", &ErrorSQLInvalid{}, false},
		{"filled", &ErrorSQLInvalid{Err: errors.New("error")}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.err.Unwrap(); (err != nil) != tt.wantErr {
				t.Errorf("ErrorSQLInvalid.Unwrap() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
