package sqlm

import (
	"database/sql/driver"
	"fmt"
)

// StringColScan unmarshal from db string column
func StringColScan(val interface{}) (string, error) {
	if val == nil {
		return "", nil
	}
	switch val := val.(type) {
	case []byte:
		return string(val), nil
	case string:
		return val, nil
	default:
		return "", fmt.Errorf("unsupported type: %T", val)
	}
}

// NullString store db string columns allow empty
type NullString string

// Scan for interface sql.Scanner
func (c *NullString) Scan(val interface{}) error {
	s, err := StringColScan(val)
	if err == nil {
		*c = NullString(s)
	}
	return err
}

// Value for interface driver.Valuer
func (c NullString) Value() (driver.Value, error) {
	return string(c), nil
}
