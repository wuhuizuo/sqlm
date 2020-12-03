package sqlm

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"

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

// Table Sql Table
type Table struct {
	*Database    `json:"database"`
	TableName    string `json:"tableName"`
	TableHooks   `json:"-"`
	*TableSchema `json:"-"`
}

// InsertHookFunc hook for table insert record operation
type InsertHookFunc func(t TableFuncInterface, record interface{}) error

// InsertsHookFunc hook for table inserts records operation
type InsertsHookFunc func(t TableFuncInterface, records []interface{}) error

// SaveHookFunc hook for table single record update operation
type SaveHookFunc func(t TableFuncInterface, record interface{}) error

// UpdateHookFunc hook for table records update operation
type UpdateHookFunc func(t TableFuncInterface, rf RowFilter, parts map[string]interface{}) error

// DeleteHookFunc hook for table delete records operation
type DeleteHookFunc func(t TableFuncInterface, rf RowFilter) error

// TableOperateHook hook for single kind operation
type TableOperateHook struct {
	Before []interface{}
	After  []interface{}
}

// TableHooks for all operation before and after for the table
type TableHooks struct {
	Insert  TableOperateHook
	Inserts TableOperateHook
	Save    TableOperateHook
	Update  TableOperateHook
	Delete  TableOperateHook
}

// Hooks return self
func (h *TableHooks) Hooks() *TableHooks {
	return h
}

// AppendHooks with other hooks
func (h *TableHooks) AppendHooks(newHooks *TableHooks) {
	if newHooks == nil {
		return
	}

	if len(newHooks.Insert.Before) > 0 {
		h.Insert.Before = append(h.Insert.Before, newHooks.Insert.Before...)
	}
	if len(newHooks.Insert.After) > 0 {
		h.Insert.After = append(h.Insert.After, newHooks.Insert.After...)
	}
	if len(newHooks.Inserts.Before) > 0 {
		h.Inserts.Before = append(h.Inserts.Before, newHooks.Inserts.Before...)
	}
	if len(newHooks.Insert.After) > 0 {
		h.Inserts.After = append(h.Inserts.After, newHooks.Inserts.After...)
	}
	if len(newHooks.Update.Before) > 0 {
		h.Update.Before = append(h.Update.Before, newHooks.Update.Before...)
	}
	if len(newHooks.Insert.After) > 0 {
		h.Update.After = append(h.Update.After, newHooks.Update.After...)
	}
	if len(newHooks.Save.Before) > 0 {
		h.Save.Before = append(h.Save.Before, newHooks.Save.Before...)
	}
	if len(newHooks.Insert.After) > 0 {
		h.Save.After = append(h.Save.After, newHooks.Save.After...)
	}
	if len(newHooks.Delete.Before) > 0 {
		h.Delete.Before = append(h.Delete.Before, newHooks.Delete.Before...)
	}
	if len(newHooks.Insert.After) > 0 {
		h.Delete.After = append(h.Delete.After, newHooks.Delete.After...)
	}
}

// TableHookInterface for hook methods interface
type TableHookInterface interface {
	Hooks() *TableHooks
	AppendHooks(*TableHooks)
}

// TableFuncInterface basic functions interface
type TableFuncInterface interface {
	TableHookInterface

	Con(...bool) *sqlx.DB
	// RowModel should return a struct point
	RowModel() interface{}
	Schema() *TableSchema
	Create() error
	Get(filter RowFilter, record interface{}) error
	List(filter RowFilter, options ListOptions) ([]interface{}, error)
	Insert(row interface{}) (int64, error)
	Inserts(rows []interface{}) ([]int64, error)
	Delete(filter RowFilter) error
	Save(interface{}) error
	Update(filter RowFilter, updateParts map[string]interface{}) error
	IsDup(row interface{}) (interface{}, error)
	ScanRow(rows *sqlx.Rows) (interface{}, error)
}

// JoinReplacer 联合查询表明替换信息
type JoinReplacer struct {
	Join                   bool
	OriginTablePlaceholder string
	TempTablePlaceholder   string
}

// ListOptions for TableFuncInterface.List()
type ListOptions struct {
	Columns       []string
	OrderByColumn string
	OrderDesc     bool
	AllColumns    bool
	Distinct      bool
	Limit         int32
}

type tableExistExec func(t TableFuncInterface) error

// Schema of table
func Schema(t TableFuncInterface, table *Table) *TableSchema {
	if table.TableSchema == nil {
		var driver string
		if db := table.Database; db != nil {
			driver = db.Driver
		}
		table.TableSchema = NewTableSchema(reflect.TypeOf(t.RowModel()))
		table.TableSchema.Driver = driver
		table.TableSchema.Name = table.TableName
	}
	return table.TableSchema
}

// Create table if not exists
func Create(t TableFuncInterface) error {
	createSQL := t.Schema().CreateSQL()
	_, err := t.Con().Exec(createSQL)
	if err != nil {
		return fmt.Errorf("%w\n sql: %s", err, createSQL)
	}
	return nil
}

// Insert records to Table
// 	if has dup keys record, then update it
func Insert(t TableFuncInterface, record interface{}) (int64, error) {
	// call before hooks
	for _, hook := range t.Hooks().Insert.Before {
		if err := hook.(InsertHookFunc)(t, record); err != nil {
			return 0, err
		}
	}

	insertID, err := insert(t, record)
	if err != nil {
		return insertID, err
	}

	// call after hooks
	for _, hook := range t.Hooks().Insert.After {
		if hookErr := hook.(InsertHookFunc)(t, record); hookErr != nil {
			return insertID, hookErr
		}
	}

	return insertID, err
}

// insert records to Table
// 	if has dup keys record, then update it
func insert(t TableFuncInterface, record interface{}) (int64, error) {
	// 针对无重复记录的情况下的插入
	var insertPatterns []string
	insertKeys := t.Schema().InsertCols()
	for _, k := range insertKeys {
		insertPatterns = append(insertPatterns, ":"+k)
	}
	insertKeysStr := strings.Join(insertKeys, ",")
	insertValPatternStr := strings.Join(insertPatterns, ",")

	// 语句组装
	var query string
	queryTpl := "INSERT INTO %s (%s) VALUES (%s)"
	targetTable, err := t.Schema().TargetName(record)
	if err != nil {
		return 0, err
	}
	query = fmt.Sprintf(queryTpl, targetTable, insertKeysStr, insertValPatternStr)
	query += insertConflictUpdatePattern(t)

	// 语句执行
	ret, err := execWithAutoCreate(t, targetTable, query, record)
	if err == nil && ret != nil {
		insertID, _ := ret.LastInsertId()
		return insertID, nil
	}

	return 0, err
}

func whenTableExist(t TableFuncInterface, exec tableExistExec) error {
	tableNotExistErrMsgReg := regexp.MustCompile(TableNotExistErrorRegex)
	err := exec(t)

	if err != nil && !tableNotExistErrMsgReg.MatchString(err.Error()) {
		return err
	}

	return nil
}

func withAutoCreate(t TableFuncInterface, targetTable string, exec tableExistExec) error {
	tableNotExistErrMsgReg := regexp.MustCompile(TableNotExistErrorRegex)
	err := exec(t)

	if err == nil || !tableNotExistErrMsgReg.MatchString(err.Error()) {
		return err
	}

	// 如果错误类型是数据表不存在,则自动创建并重调
	schema := *t.Schema()
	schema.Name = targetTable
	createSQL := schema.CreateSQL()
	_, err = t.Con().Exec(createSQL)
	if err != nil {
		errTpl := "try to auto create table (%s) failed:\nsql: %s\nerror: %v"
		return fmt.Errorf(errTpl, targetTable, createSQL, err)
	}

	// 表创建成功后重新执行
	err = exec(t)
	if err != nil && !tableNotExistErrMsgReg.MatchString(err.Error()) {
		return fmt.Errorf("error also happened after table auto created: %w", err)
	}
	return err
}

func execWhenExist(t TableFuncInterface, query string, arg interface{}) (ret sql.Result, err error) {
	exec := func(et TableFuncInterface) error {
		ret, err = et.Con().NamedExec(query, arg)
		return err
	}

	err = whenTableExist(t, exec)
	return ret, err
}

func execWithAutoCreate(t TableFuncInterface, table, query string, arg interface{}) (ret sql.Result, err error) {
	exec := func(et TableFuncInterface) error {
		ret, err = et.Con().NamedExec(query, arg)
		return err
	}

	err = withAutoCreate(t, table, exec)
	return ret, err
}

func queryWhenExist(t TableFuncInterface, query string, arg interface{}) (rows *sqlx.Rows, err error) {
	exec := func(et TableFuncInterface) error {
		// nolint: rowserrcheck
		rows, err = et.Con().NamedQuery(query, arg)
		return err
	}

	err = whenTableExist(t, exec)
	return rows, err
}

func insertConflictUpdatePattern(t TableFuncInterface) string {
	schema := t.Schema()
	dupUpdatePattern := schema.UpdatePatternsWhenDup()
	var conflictUpdateTpl string
	if len(dupUpdatePattern) > 0 {
		switch schema.Driver {
		case DriverMysql:
			conflictUpdateTpl = " ON DUPLICATE KEY UPDATE %s"
			return fmt.Sprintf(conflictUpdateTpl, dupUpdatePattern)
		case DriverSQLite, DriverSQLite3:
			conflictUpdateTpl = " ON CONFLICT(%s) DO UPDATE SET %s"
			return fmt.Sprintf(conflictUpdateTpl, strings.Join(schema.PrimaryCols(), ","), dupUpdatePattern)
		default:
			return conflictUpdateTpl
		}
	}

	return conflictUpdateTpl
}

// IsDup record in table
func IsDup(t TableFuncInterface, row interface{}) (interface{}, error) {
	whereFormatter := UniqWhereFormatter(t)
	targetTable, err := t.Schema().TargetName(row)
	if err != nil {
		return nil, err
	}
	query := fmt.Sprintf("select %s from %s", strings.Join(t.Schema().ColNames(true), ","), targetTable)
	if whereFormatter != "" {
		query += " where " + whereFormatter
	}
	rows, queryErr := t.Con().NamedQuery(query, row)
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
func Inserts(t TableFuncInterface, records []interface{}) ([]int64, error) {
	// call before hooks
	for _, hook := range t.Hooks().Inserts.Before {
		if err := hook.(InsertsHookFunc)(t, records); err != nil {
			return nil, err
		}
	}

	ret, err := inserts(t, records)
	if err != nil {
		return ret, err
	}

	// call after hooks
	for _, hook := range t.Hooks().Inserts.After {
		if hookErr := hook.(InsertsHookFunc)(t, records); hookErr != nil {
			return ret, hookErr
		}
	}

	return ret, err
}

func inserts(t TableFuncInterface, records []interface{}) ([]int64, error) {
	ret := make([]int64, 0)
	for _, r := range records {
		id, err := insert(t, r)
		if err != nil {
			return ret, err
		}
		ret = append(ret, id)
	}
	return ret, nil
}

// Save the exist record
func Save(t TableFuncInterface, record interface{}) error {
	// call before hooks
	for _, hook := range t.Hooks().Save.Before {
		if err := hook.(SaveHookFunc)(t, record); err != nil {
			return err
		}
	}

	err := save(t, record)
	if err != nil {
		return err
	}

	// call after hooks
	for _, hook := range t.Hooks().Save.After {
		if hookErr := hook.(SaveHookFunc)(t, record); hookErr != nil {
			return hookErr
		}
	}

	return nil
}

func save(t TableFuncInterface, record interface{}) error {
	// 更新部分组装
	updateFields := t.Schema().UpdateColsWhenDup()
	var updatePatterns []string
	for _, k := range updateFields {
		updatePatterns = append(updatePatterns, k+"=:"+k)
	}

	// 过滤条件组装
	var wherePatterns []string
	var pCols []string
	idKey := t.Schema().KeyCol()
	if idKey != "" {
		pCols = append(pCols, idKey)
	} else {
		pCols = t.Schema().PrimaryCols()
	}
	if len(pCols) == 0 {
		return &ErrorSQLInvalid{Message: "table schema should has one key col or primary col setted"}
	}
	for _, k := range pCols {
		wherePatterns = append(wherePatterns, k+"=:"+k)
	}

	// 整体语句组合
	targetTable, err := t.Schema().TargetName(record)
	if err != nil {
		return err
	}
	sets := strings.Join(updatePatterns, ",")
	whereConditionStr := strings.Join(wherePatterns, " AND ")
	query := fmt.Sprintf("%s %s %s %s %s %s", SQLKeyUpdate, targetTable, SQLKeySet, sets, SQLKeyWhere, whereConditionStr)

	// 执行
	_, execErr := t.Con().NamedExec(query, record)
	return execErr
}

// update records in Table
func update(t TableFuncInterface, filter RowFilter, updateData interface{}, updateFields []string) (int64, error) {
	var rowsAffect int64

	// 计算过滤条件
	whereFormatter, err := composeWhereConditionStrForUpdate(filter)
	if err != nil {
		return rowsAffect, err
	}

	// 计算更新内容
	var updatePatterns []string
	for _, k := range updateFields {
		updatePatterns = append(updatePatterns, k+"=:"+k)
	}

	// 组合sql语句
	targetTable, err := t.Schema().TargetName(filter)
	if err != nil {
		return 0, err
	}
	query := fmt.Sprintf("%s %s %s %s", SQLKeyUpdate, targetTable, SQLKeySet, strings.Join(updatePatterns, ","))
	if whereFormatter != "" {
		query += " where " + whereFormatter
	}

	// 执行
	ret, execErr := execWhenExist(t, query, updateData)
	if ret != nil {
		rowsAffect, _ = ret.RowsAffected()
	}
	return rowsAffect, execErr
}

func composeWhereConditionStrForUpdate(filter RowFilter) (string, error) {
	var ret string
	where, err := filter.WherePattern()
	if err != nil {
		return ret, &ErrorSQLInvalid{"where条件组装失败", err}
	}
	if where == nil {
		return ret, nil
	}
	if where.Join != nil {
		return ret, &ErrorSQLInvalid{Message: "update中的不允许where中存在联合条件"}
	}

	ret = where.Format
	for k, v := range where.Patterns {
		re := regexp.MustCompile(":" + k + `\b`)
		ret = re.ReplaceAllString(ret, formatCondition(v))
	}

	return ret, nil
}

func formatCondition(v interface{}) string {
	switch v := v.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return fmt.Sprintf("%v", v)
	case bool:
		if v {
			return "1"
		}
		return "0"
	default:
		return fmt.Sprintf("'%v'", v)
	}
}

func loadDataForUpdate(t TableFuncInterface, src map[string]interface{}, dest interface{}) ([]string, error) {
	var updateFields []string

	if dest == nil {
		for k := range src {
			updateFields = append(updateFields, k)
		}
		return updateFields, nil
	}

	bs, _ := json.Marshal(&src)
	if err := json.Unmarshal(bs, dest); err != nil {
		return updateFields, err
	}

	for _, c := range t.Schema().Columns {
		if _, ok := src[c.JSONName]; ok {
			updateFields = append(updateFields, c.Name)
		}
	}
	return updateFields, nil
}

// Update records in Table
func Update(t TableFuncInterface, filter RowFilter, updateParts map[string]interface{}) error {
	if len(updateParts) == 0 {
		return nil
	}

	// call before hooks
	for _, hook := range t.Hooks().Update.Before {
		if err := hook.(UpdateHookFunc)(t, filter, updateParts); err != nil {
			return err
		}
	}

	updateModel := t.RowModel()
	updateFields, err := loadDataForUpdate(t, updateParts, updateModel)
	if err != nil {
		return &ErrorSQLInvalid{"invalid update parts", err}
	}
	_, err = update(t, filter, updateModel, updateFields)

	if err != nil {
		return err
	}

	// call after hooks
	for _, hook := range t.Hooks().Update.After {
		if hookErr := hook.(UpdateHookFunc)(t, filter, updateParts); hookErr != nil {
			return hookErr
		}
	}

	return nil
}

// Delete records in Table
func Delete(t TableFuncInterface, filter RowFilter) error {
	// call before hooks
	for _, hook := range t.Hooks().Delete.Before {
		if err := hook.(DeleteHookFunc)(t, filter); err != nil {
			return err
		}
	}

	if _, err := delete(t, filter); err != nil {
		return err
	}

	// call after hooks
	for _, hook := range t.Hooks().Delete.After {
		if err := hook.(DeleteHookFunc)(t, filter); err != nil {
			return err
		}
	}

	return nil
}

func delete(t TableFuncInterface, filter RowFilter) (sql.Result, error) {
	where, err := filter.WherePattern()
	if err != nil {
		return nil, &ErrorSQLInvalid{"where条件组装失败", err}
	}
	if where == nil || where.Format == "" {
		return nil, &ErrorSQLInvalid{Message: "不允许不带where的删除操作"}
	}
	if where.Join != nil {
		return nil, &ErrorSQLInvalid{Message: "delete中的不允许where中存在联合条件"}
	}

	// 组合sql语句并执行
	targetTable, err := t.Schema().TargetName(filter)
	if err != nil {
		return nil, err
	}
	query := fmt.Sprintf("%s %s %s %s %s", SQLKeyDelete, SQLKeyFrom, targetTable, SQLKeyWhere, where.Format)
	return execWhenExist(t, query, where.Patterns)
}

// List Records from Table
func List(t TableFuncInterface, filter RowFilter, options ListOptions) ([]interface{}, error) {
	records := make([]interface{}, 0)
	query, wherePatterns, err := t.Schema().SelectSQL(filter, options)
	if err != nil {
		return records, err
	}

	rows, queryErr := queryWhenExist(t, query.String(), wherePatterns)
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
func GetFirst(t TableFuncInterface, filter RowFilter, record interface{}) error {
	query, wherePatterns, err := t.Schema().SelectSQL(filter, ListOptions{AllColumns: true, Limit: 1})
	if err != nil {
		return err
	}

	rows, queryErr := queryWhenExist(t, query.String(), wherePatterns)
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
func ScanRow(t TableFuncInterface, rows *sqlx.Rows) (interface{}, error) {
	record := t.RowModel()
	err := rows.StructScan(record)
	return record, err
}
