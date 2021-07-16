package sqlm

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/jmoiron/sqlx"
)

// TableNotExistErrorRegex for table not exist db response
const TableNotExistErrorRegex = `[tT]able\s+.+\s+doesn't\s+exist`

// SQL 关键字
//	 这个作为基础库需要支持灵活的sql查询/操作,用占位符的方式满足不了这个要求。
//   CodeCC在这里扫描在这里遇到sql拼装或者格式化组合时会告警。
const (
	SQLKeyUpdate = "UPDATE"
	SQLKeySet    = "SET"
	SQLKeyDelete = "DELETE"
	SQLKeyWhere  = "WHERE"
	SQLKeyFrom   = "FROM"
)

// JoinReplacer 联合查询表明替换信息
type JoinReplacer struct {
	Join                   bool
	OriginTablePlaceholder string
	TempTablePlaceholder   string
}

// ListOptions for Table#List()
type ListOptions struct {
	Columns       []string
	OrderByColumn string
	OrderDesc     bool
	AllColumns    bool
	Distinct      bool
	Limit         int32
}

// Table Sql Table
type Table struct {
	*Database  `json:"database"`
	TableName  string `json:"tableName"`
	TableHooks `json:"-"`
	schema     *TableSchema
	rowModeler func() interface{}

	once sync.Once
}

// RowModel get model for store
func (t *Table) RowModel() interface{} {
	if t.rowModeler == nil {
		return nil
	}

	return t.rowModeler()
}

// SetRowModel set model for store
func (t *Table) SetRowModel(modeler func() interface{}) {
	t.rowModeler = modeler
}

// Schema of table
func (t *Table) Schema() *TableSchema {
	t.once.Do(t.initSchema)

	return t.schema
}

// Create table if not exists
func (t *Table) Create() error {
	con, err := t.Con()
	if err != nil {
		return err
	}

	createSQL := t.Schema().CreateSQL()
	_, err = con.Exec(createSQL)
	if err != nil {
		return fmt.Errorf("%w\n sql: %s", err, createSQL)
	}

	return nil
}

// Insert records to Table
// 	if has dup keys record, then update it
func (t *Table) Insert(record interface{}) (int64, error) {
	// call before hooks.
	for _, hook := range t.TableHooks.Insert.Before {
		if err := hook.(InsertHookFunc)(t, record); err != nil {
			return 0, err
		}
	}

	insertID, err := t.insert(record)
	if err != nil {
		return insertID, err
	}

	// call after hooks.
	for _, hook := range t.TableHooks.Insert.After {
		if hookErr := hook.(InsertHookFunc)(t, record); hookErr != nil {
			return insertID, hookErr
		}
	}

	return insertID, err
}

// IsDup record in table
func (t *Table) IsDup(row interface{}) (interface{}, error) {
	whereFormatter := UniqWhereFormatter(t)
	targetTable, err := t.Schema().TargetName(row)
	if err != nil {
		return nil, err
	}
	query := fmt.Sprintf("select %s from %s", strings.Join(t.Schema().ColNames(true), ","), targetTable)
	if whereFormatter != "" {
		query += " where " + whereFormatter
	}

	con, err := t.Con()
	if err != nil {
		return false, err
	}

	rows, queryErr := con.NamedQuery(query, row)
	if queryErr != nil || rows == nil {
		return false, queryErr
	}
	defer rows.Close()

	var records []interface{}
	for rows.Next() {
		record, err := t.ScanRow(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	switch len(records) {
	case 0:
		return nil, nil
	case singlePKCount:
		return records[0], nil
	default:
		return nil, fmt.Errorf("record count with same primary keys > 1, maybe your table schema is not sync with db")
	}
}

// Inserts records to Table
func (t *Table) Inserts(records []interface{}) ([]int64, error) {
	// call before hooks
	for _, hook := range t.TableHooks.Inserts.Before {
		if err := hook.(InsertsHookFunc)(t, records); err != nil {
			return nil, err
		}
	}

	ret, err := t.inserts(records)
	if err != nil {
		return ret, err
	}

	// call after hooks
	for _, hook := range t.TableHooks.Inserts.After {
		if hookErr := hook.(InsertsHookFunc)(t, records); hookErr != nil {
			return ret, hookErr
		}
	}

	return ret, err
}

// Save the exist record
func (t *Table) Save(record interface{}) error {
	// call before hooks
	for _, hook := range t.TableHooks.Save.Before {
		if err := hook.(SaveHookFunc)(t, record); err != nil {
			return err
		}
	}

	err := t.save(record)
	if err != nil {
		return err
	}

	// call after hooks
	for _, hook := range t.TableHooks.Save.After {
		if hookErr := hook.(SaveHookFunc)(t, record); hookErr != nil {
			return hookErr
		}
	}

	return nil
}

// Update records in Table
func (t *Table) Update(filter RowFilter, updateParts map[string]interface{}) error {
	if len(updateParts) == 0 {
		return nil
	}

	// call before hooks
	for _, hook := range t.TableHooks.Update.Before {
		if err := hook.(UpdateHookFunc)(t, filter, updateParts); err != nil {
			return err
		}
	}

	updatePayload := t.RowModel()
	updateFields, err := loadDataForUpdate(t, updateParts, updatePayload)
	if err != nil {
		return &ErrorSQLInvalid{"invalid update parts", err}
	}

	_, err = t.update(filter, updatePayload, updateFields)
	if err != nil {
		return err
	}

	// call after hooks
	for _, hook := range t.TableHooks.Update.After {
		if hookErr := hook.(UpdateHookFunc)(t, filter, updateParts); hookErr != nil {
			return hookErr
		}
	}

	return nil
}

// Delete records in Table
func (t *Table) Delete(filter RowFilter) error {
	// call before hooks
	for _, hook := range t.TableHooks.Delete.Before {
		if err := hook.(DeleteHookFunc)(t, filter); err != nil {
			return err
		}
	}

	if _, err := t.deleteRows(filter); err != nil {
		return err
	}

	// call after hooks
	for _, hook := range t.TableHooks.Delete.After {
		if err := hook.(DeleteHookFunc)(t, filter); err != nil {
			return err
		}
	}

	return nil
}

// List Records from Table
func (t *Table) List(filter RowFilter, options ListOptions) ([]interface{}, error) {
	records := make([]interface{}, 0)
	query, wherePatterns, err := t.Schema().SelectSQL(filter, options)
	if err != nil {
		return records, err
	}

	rows, queryErr := t.queryWhenExist(query.String(), wherePatterns)
	if queryErr != nil {
		return records, fmt.Errorf("query failed :%w\nsql: %s\nwherePatterns: %v", queryErr, &query, wherePatterns)
	}
	if rows == nil {
		return records, nil
	}

	// 释放db连接
	defer rows.Close()

	for rows.Next() {
		record, err := t.ScanRow(rows)
		if err != nil {
			fmt.Printf("sqlx scan error: %s\n%s\n", err.Error(), &query)
			return records, err
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return records, nil
}

// GetFirst Record from Table by filter
func (t *Table) Get(filter RowFilter, record interface{}) error {
	query, wherePatterns, err := t.Schema().SelectSQL(filter, ListOptions{AllColumns: true, Limit: 1})
	if err != nil {
		return err
	}

	rows, queryErr := t.queryWhenExist(query.String(), wherePatterns)
	if queryErr != nil {
		return fmt.Errorf("query failed :%w\nsql: %s\nwherePatterns: %v", queryErr, &query, wherePatterns)
	}

	// 释放db连接
	defer rows.Close()

	if !rows.Next() {
		return sql.ErrNoRows
	}
	if err := rows.Err(); err != nil {
		return err
	}

	return rows.StructScan(record)
}

// ScanRow scan struct from table row
func (t *Table) ScanRow(rows *sqlx.Rows) (interface{}, error) {
	record := t.RowModel()
	err := rows.StructScan(record)
	return record, err
}
