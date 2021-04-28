package sqlm

import (
	"github.com/jmoiron/sqlx"
)

// testRecord record for unit testing
// +--------------+---------------+------+-----+-------------------+----------------+------------+
// | Field        | Type          | Null | Key | Default           | Extra          | 备注       |
// +--------------+---------------+------+-----+-------------------+----------------+------------+
// | id           | int(11)       | NO   | MUL | NULL              | auto_increment |告警ID      |
// | projectId    | int(11)       | NO   | PRI | NULL              |                |项目ID      |
// | ruleId       | int(11)       | NO   | PRI | NULL              |                |告警规则ID  |
// | sendStatus   | tinyint(4)    | YES  |     | 0                 |                |告警状态    |
// | ensureStatus | tinyint(4)    | YES  |     | 0                 |                |告警确认状态 |
// | ensureUser   | varchar(32)   | YES  |     | NULL              |                |告警确认人   |
// | ensureTime   | datetime      | YES  |     | NULL              |                |确认时间     |
// | createtime   | datetime      | NO   | PRI | CURRENT_TIMESTAMP |                |告警时间     |
// | title        | varchar(128)  | NO   |     | NULL              |                |告警标题     |
// | body         | varchar(1024) | NO   |     | NULL              |                |告警内容     |
// +--------------+---------------+------+-----+-------------------+----------------+------------+
type testRecord struct {
	ID           int32      `json:"id,omitempty"           db:"id,type=INT,auto_increment,key"`
	ProjectID    int32      `json:"projectId"              db:"projectId,type=INT,not_null,split"`
	RuleID       int32      `json:"ruleId,omitempty"       db:"ruleId,type=INT,not_null,primary"`
	CreateTime   Datetime   `json:"createtime,omitempty"   db:"createtime,type=DATETIME,default=CURRENT_TIMESTAMP,primary"`
	SendStatus   uint8      `json:"sendStatus,omitempty"   db:"sendStatus,type=TINYINT,default=0,not_insert"`
	EnsureUser   NullString `json:"ensureUser,omitempty"   db:"ensureUser,type=VARCHAR(32),not_insert"`
	EnsureStatus uint8      `json:"ensureStatus,omitempty" db:"ensureStatus,type=TINYINT,default=0,not_insert"`
	EnsureTime   Datetime   `json:"ensureTime,omitempty"   db:"ensureTime,type=DATETIME,not_insert"`
	Title        string     `json:"title,omitempty"        db:"title,type=VARCHAR(128),not_null"`
	Body         string     `json:"body,omitempty"         db:"body,type=VARCHAR(1024),not_null"`
}

// testTable for unit testing
type testTable Table

// RowModel model for store
func (t *testTable) RowModel() interface{} {
	return &testRecord{}
}

// Schema of table
func (t *testTable) Schema() *TableSchema {
	return Schema(t, (*Table)(t))
}

// Create of table
func (t *testTable) Create() error {
	return Create(t)
}

// IsDup record in Table?
func (t *testTable) IsDup(record interface{}) (interface{}, error) {
	return IsDup(t, record)
}

// Insert record to Table
func (t *testTable) Insert(record interface{}) (int64, error) {
	return Insert(t, record)
}

// Inserts records to Table
func (t *testTable) Inserts(records []interface{}) ([]int64, error) {
	return Inserts(t, records)
}

// Delete records from Table
func (t *testTable) Delete(filter RowFilter) error {
	return Delete(t, filter)
}

// Save record
func (t *testTable) Save(record interface{}) error {
	return Save(t, record)
}

// Update records in Table
func (t *testTable) Update(filter RowFilter, updateParts map[string]interface{}) error {
	return Update(t, filter, updateParts)
}

// List records from Table
func (t *testTable) List(filter RowFilter, options ListOptions) ([]interface{}, error) {
	return List(t, filter, options)
}

// Get record by Key
func (t *testTable) Get(filter RowFilter, record interface{}) error {
	return GetFirst(t, filter, record)
}

// ScanRow scan struct from table row
func (t *testTable) ScanRow(rows *sqlx.Rows) (interface{}, error) {
	return ScanRow(t, rows)
}
