package sqlm

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/jmoiron/sqlx"
)

type dbOptionSetter func(*sqlx.DB)

// tableCreate 创建数据表
func tableCreate(tableField reflect.Value) (*sqlx.DB, error) {
	if !tableField.CanInterface() {
		return nil, errors.New("table field can not be used without panicking")
	}

	tableCreateRet := tableField.MethodByName("Create").Call([]reflect.Value{})
	err := tableCreateRet[0].Interface()
	if err != nil {
		return nil, err.(error)
	}

	return tableDBCon(tableField)
}

// tableDBCon 数据收集库连接,用于接下来的初始化
func tableDBCon(tableField reflect.Value) (*sqlx.DB, error) {
	dbConRet := tableField.MethodByName("Con").Call([]reflect.Value{})
	if len(dbConRet) == 0 {
		return nil, errors.New("db con none returns")
	}
	dbCon := dbConRet[0].Interface()
	if dbCon == nil {
		return nil, errors.New("db con failed")
	}
	con, ok := dbCon.(*sqlx.DB)
	if !ok {
		return nil, fmt.Errorf("return type (%T) not matched, expect *sqlx.DB", dbCon)
	}
	if con == nil {
		return nil, errors.New("return nil")
	}

	return con, nil
}
