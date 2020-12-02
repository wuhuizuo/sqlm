package sqlm

import (
	"github.com/jmoiron/sqlx"
)

// Group user group account model
type Group struct {
	ID          int32      `json:"id,omitempty" db:"id,type=INT,auto_increment,key"`         // 告警ID
	Name        string     `json:"name"         db:"name,type=VARCHAR(32),not_null,primary"` // 用户组名
	Authority   int32      `json:"authority"    db:"authority,type=INT,default"`             // 预留
	Description NullString `json:"description"  db:"description,type=VARCHAR(1024)"`         // 用户组描述
}

// GroupTable for store right group
type GroupTable Table

// RowModel model for store
func (t *GroupTable) RowModel() interface{} {
	return &Group{}
}

// Schema of table
func (t *GroupTable) Schema() *TableSchema {
	return Schema(t, (*Table)(t))
}

// Create of table
func (t *GroupTable) Create() error {
	return Create(t)
}

// IsDup record in Table?
func (t *GroupTable) IsDup(record interface{}) (interface{}, error) {
	return IsDup(t, record)
}

// Insert record to Table
func (t *GroupTable) Insert(record interface{}) (int64, error) {
	return Insert(t, record)
}

// Inserts records to Table
func (t *GroupTable) Inserts(records []interface{}) ([]int64, error) {
	return Inserts(t, records)
}

// Delete records from Table
func (t *GroupTable) Delete(filter RowFilter) error {
	return Delete(t, filter)
}

// Save record
func (t *GroupTable) Save(record interface{}) error {
	return Save(t, record)
}

// Update records in Table
func (t *GroupTable) Update(filter RowFilter, updateParts map[string]interface{}) error {
	return Update(t, filter, updateParts)
}

// List records from Table
func (t *GroupTable) List(filter RowFilter, options ListOptions) ([]interface{}, error) {
	return List(t, filter, options)
}

// Get record by Key
func (t *GroupTable) Get(filter RowFilter, record interface{}) error {
	return GetFirst(t, filter, record)
}

// ScanRow scan struct from table row
func (t *GroupTable) ScanRow(rows *sqlx.Rows) (interface{}, error) {
	return ScanRow(t, rows)
}
