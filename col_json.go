package sqlm

import (
	"crypto/md5"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"net/url"
)

// JSONColScan unmarshal from db JSON column
func JSONColScan(val, dest interface{}) error {
	if val == nil {
		return nil
	}
	switch val := val.(type) {
	case []byte:
		return json.Unmarshal(val, dest)
	case string:
		return json.Unmarshal([]byte(val), dest)
	default:
		return fmt.Errorf("unsupported type: %T", val)
	}
}

// StringList string list col
type StringList []string

// Scan for interface sql.Scanner
func (c *StringList) Scan(val interface{}) error {
	return JSONColScan(val, c)
}

// Value for interface driver.Valuer
func (c StringList) Value() (driver.Value, error) {
	return json.Marshal(c)
}

// ValList list col
type ValList []interface{}

// Scan for interface sql.Scanner
func (c *ValList) Scan(val interface{}) error {
	return JSONColScan(val, c)
}

// Value for interface driver.Valuer
func (c ValList) Value() (driver.Value, error) {
	return json.Marshal(c)
}

// URLQueryValues query params for http request
type URLQueryValues url.Values

// Scan for interface sql.Scanner
func (c *URLQueryValues) Scan(val interface{}) error {
	return JSONColScan(val, c)
}

// Value for interface driver.Valuer
func (c URLQueryValues) Value() (driver.Value, error) {
	return json.Marshal(c)
}

// HashCol hash col
type HashCol map[string]interface{}

// Scan for interface sql.Scanner
func (c *HashCol) Scan(val interface{}) error {
	return JSONColScan(val, c)
}

// Value for interface driver.Valuer
func (c HashCol) Value() (driver.Value, error) {
	return json.Marshal(c)
}

// MD5 for the uniq primary key
func (c *HashCol) MD5() string {
	return dataMD5(c)
}

// dataMD5 create a md5sum key for a given data's json
func dataMD5(data interface{}) string {
	bs, err := json.Marshal(data)
	if err != nil {
		return ""
	}

	return fmt.Sprintf("%x", md5.Sum(bs))
}

// UserRightMatrix 用户权限矩阵 project => group role
type UserRightMatrix struct {
	ProjectGroupMatrix map[uint64]string `json:"matrix,omitempty"`       // 具体加入项目分配的权限组
	DefaultGroup       string            `json:"defaultGroup,omitempty"` // 兜底权限组,也就是如果没有加入任何项目时所具备的权限
	Blocked            bool              `json:"blocked,omitempty"`      // 用户封禁
}

// Scan for interface sql.Scanner
func (c *UserRightMatrix) Scan(val interface{}) error {
	return JSONColScan(val, c)
}

// Value for interface driver.Valuer
func (c UserRightMatrix) Value() (driver.Value, error) {
	return json.Marshal(c)
}
