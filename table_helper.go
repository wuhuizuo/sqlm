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

type targetQuery struct {
	targetTable string
	query       string
}

func (t *Table) initSchema() {
	if t.rowModeler == nil {
		return
	}

	t.schema = NewTableSchema(reflect.TypeOf(t.rowModeler()))
	var driver string
	if db := t.Database; db != nil {
		driver = db.Driver
	}
	t.schema.Driver = driver
	t.schema.Name = t.TableName
}

func (t *Table) inserts(records []interface{}) ([]int64, error) {
	ret := make([]int64, 0)
	for _, r := range records {
		id, err := t.insert(r)
		if err != nil {
			return ret, err
		}

		ret = append(ret, id)
	}

	return ret, nil
}

// insert records to table.
// 	if has dup keys record, then return error.
func (t *Table) insert(record interface{}) (int64, error) {
	insertQuery, err := t.composeInsertQuery(record)
	if err != nil {
		return 0, err
	}

	// 语句执行
	ret, err := t.execWithAutoCreate(insertQuery, record)
	if err == nil && ret != nil {
		insertID, _ := ret.LastInsertId()
		return insertID, nil
	}

	return 0, err
}

func (t *Table) composeInsertQuery(record interface{}) (*targetQuery, error) {
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
		return nil, err
	}
	query = fmt.Sprintf(queryTpl, targetTable, insertKeysStr, insertValPatternStr)

	return &targetQuery{targetTable, query}, nil
}

func (t *Table) save(record interface{}) error {
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
		var err error
		pCols, err = t.Schema().PrimaryCols()
		if err != nil {
			return err
		}
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

	con, err := t.Con()
	if err != nil {
		return err
	}

	// 执行
	_, execErr := con.NamedExec(query, record)
	return execErr
}

// update records in Table.
func (t *Table) update(filter RowFilter, updatePayload interface{}, updateFields []string) (int64, error) {
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
	ret, execErr := t.execWhenExist(query, updatePayload)
	if ret != nil {
		rowsAffect, _ = ret.RowsAffected()
	}
	return rowsAffect, execErr
}

func (t *Table) deleteRows(filter RowFilter) (sql.Result, error) {
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

	return t.execWhenExist(query, where.Patterns)
}

func (t *Table) execWhenExist(query string, arg interface{}) (ret sql.Result, err error) {
	exec := func(et *Table) error {
		con, conErr := et.Con()
		if conErr == nil {
			ret, conErr = con.NamedExec(query, arg)
		}

		return conErr
	}

	err = doWhenTableExist(t, exec)
	return ret, err
}

func (t *Table) execWithAutoCreate(query *targetQuery, arg interface{}) (ret sql.Result, err error) {
	exec := func(et *Table) error {
		con, errCon := et.Con()
		if errCon == nil {
			ret, errCon = con.NamedExec(query.query, arg)
		}

		return errCon
	}

	err = doWithAutoCreate(t, query.targetTable, exec)
	return ret, err
}

func doWhenTableExist(t *Table, do func(t *Table) error) error {
	tableNotExistErrMsgReg := regexp.MustCompile(TableNotExistErrorRegex)
	err := do(t)

	if err != nil && !tableNotExistErrMsgReg.MatchString(err.Error()) {
		return err
	}

	return nil
}

func doWithAutoCreate(t *Table, targetTable string, do func(t *Table) error) error {
	tableNotExistErrMsgReg := regexp.MustCompile(TableNotExistErrorRegex)

	err := do(t)
	if err == nil || !tableNotExistErrMsgReg.MatchString(err.Error()) {
		return err
	}

	// 如果错误类型是数据表不存在,则自动创建并重调
	schema := *t.Schema()
	schema.Name = targetTable
	createSQL := schema.CreateSQL()

	con, err := t.Con()
	if err != nil {
		return err
	}

	_, err = con.Exec(createSQL)
	if err != nil {
		errTpl := "try to auto create table (%s) failed:\nsql: %s\nerror: %w"
		return fmt.Errorf(errTpl, targetTable, createSQL, err)
	}

	// 表创建成功后重新执行
	err = do(t)
	if err != nil && !tableNotExistErrMsgReg.MatchString(err.Error()) {
		return fmt.Errorf("error also happened after table auto created: %w", err)
	}

	return err
}

func (t *Table) queryWhenExist(query string, arg interface{}) (rows *sqlx.Rows, err error) {
	exec := func(et *Table) error {
		con, errCon := et.Con()
		if errCon == nil {
			rows, errCon = con.NamedQuery(query, arg)
		}

		return errCon
	}

	err = doWhenTableExist(t, exec)
	return rows, err
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

func loadDataForUpdate(t *Table, src map[string]interface{}, dest interface{}) ([]string, error) {
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
