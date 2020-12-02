package sqlm

import (
	"github.com/jmoiron/sqlx"
)

// User user accout model
type User struct {
	// 告警ID
	ID int32 `db:"id,type=INT,auto_increment,key" json:"id,omitempty" uri:"id"`
	// User name| API KEY
	Username string `db:"username,type=VARCHAR(32),not_null,primary" json:"username" form:"username" url:"username"`
	// 用户类型: 0 为普通用户
	Type int32 `db:"type,type=INT,not_null" json:"type" form:"type"`
	// 密码(加密后), 只有在普通用户时才需要
	Password NullString `db:"password,type=VARCHAR(64),complex" binding:"max=64" json:"password" form:"password"`
	// 用户相关属性,可用来存储归属项目和角色
	Attr UserRightMatrix `db:"attr,type=TEXT" json:"attr,omitempty" form:"attr"`
	// 邮箱
	EMail NullString `db:"email,type=VARCHAR(64)" binding:"max=64" json:"email" form:"email"`
	// 手机号码
	Phone NullString `db:"phone,type=VARCHAR(16)" binding:"max=16" json:"phone" form:"phone"`
	// 用户描述
	Description NullString `db:"description,type=VARCHAR(128)" binding:"max=128" json:"description" form:"description"`
}

// UserTable for store user
type UserTable Table

// RowModel model for store
func (t *UserTable) RowModel() interface{} {
	return &User{}
}

// Schema of table
func (t *UserTable) Schema() *TableSchema {
	return Schema(t, (*Table)(t))
}

// Create of table
func (t *UserTable) Create() error {
	return Create(t)
}

// IsDup record in Table?
func (t *UserTable) IsDup(record interface{}) (interface{}, error) {
	return IsDup(t, record)
}

// Insert record to Table
func (t *UserTable) Insert(record interface{}) (int64, error) {
	return Insert(t, record)
}

// Inserts records to Table
func (t *UserTable) Inserts(records []interface{}) ([]int64, error) {
	return Inserts(t, records)
}

// Delete records from Table
func (t *UserTable) Delete(filter RowFilter) error {
	return Delete(t, filter)
}

// Save record
func (t *UserTable) Save(record interface{}) error {
	return Save(t, record)
}

// Update records in Table
func (t *UserTable) Update(filter RowFilter, updateParts map[string]interface{}) error {
	return Update(t, filter, updateParts)
}

// List records from Table
func (t *UserTable) List(filter RowFilter, options ListOptions) ([]interface{}, error) {
	return List(t, filter, options)
}

// Get record by Key
func (t *UserTable) Get(filter RowFilter, record interface{}) error {
	return GetFirst(t, filter, record)
}

// ScanRow scan struct from table row
func (t *UserTable) ScanRow(rows *sqlx.Rows) (interface{}, error) {
	return ScanRow(t, rows)
}
