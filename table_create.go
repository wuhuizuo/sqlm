package sqlm

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/jmoiron/sqlx"
)

type dbOptionSetter func(*sqlx.DB)

// DBCreateIterStructField 遍历配置模型的各个配置属性创建数据表
func DBCreateIterStructField(val reflect.Value, optionSetter dbOptionSetter) error {
	// 依次遍历各个数据表进行初始化
	dbCache := map[*sqlx.DB]bool{}
	for i := 0; i < val.NumField(); i++ {
		vf := val.Field(i)
		if vf.IsNil() || !vf.CanAddr() ||
			vf.MethodByName("Init").IsZero() ||
			vf.MethodByName("Create").IsZero() {
			continue
		}

		dbCon, err := tableCreate(vf)
		if err != nil {
			return err
		}

		dbCache[dbCon] = true
	}

	// 对每个数据库都这只最大连接数和 连接生命周期
	if optionSetter == nil {
		return nil
	}
	for k := range dbCache {
		optionSetter(k)
	}

	return nil
}

// tableCreate 创建数据表
func tableCreate(tableField reflect.Value) (*sqlx.DB, error) {
	if !tableField.CanInterface() {
		return nil, errors.New("table field can not be used without panicking")
	}

	// 初始化连接数据库, 如果没有则创建
	dbInitRet := tableField.MethodByName("Init").Call([]reflect.Value{reflect.ValueOf(true)})
	err := dbInitRet[0].Interface()
	if err != nil {
		return nil, err.(error)
	}

	// 创建表
	tableCreateRet := tableField.MethodByName("Create").Call([]reflect.Value{})
	err = tableCreateRet[0].Interface()
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
