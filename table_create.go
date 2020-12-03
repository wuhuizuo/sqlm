package sqlm

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"

	"github.com/jmoiron/sqlx"
)

// SQLiteMemroy SQLite build-in memroy
const SQLiteMemroy = ":memroy:"

type dbOptionSetter func(*sqlx.DB)

// DBCreateIterStructField 遍历配置模型的各个配置属性创建数据表
func DBCreateIterStructField(val reflect.Value, optionSetter dbOptionSetter) error {
	// 依次遍历各个数据表进行初始化
	dbCache := map[*sqlx.DB]bool{}
	for i := 0; i < val.NumField(); i++ {
		vf := val.Field(i)
		if vf.IsNil() || !vf.CanAddr() ||
			vf.MethodByName("SetOpenImplement").IsZero() ||
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
	tableField.CanInterface()
	// 注册自定义数据库打开方法
	customDBOpenImp := reflect.ValueOf(dbOpenImplement)
	tableField.MethodByName("SetOpenImplement").Call([]reflect.Value{customDBOpenImp})

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
		return nil, fmt.Errorf("db con none returns")
	}
	dbCon := dbConRet[0].Interface()
	if dbCon == nil {
		return nil, fmt.Errorf("db con failed")
	}
	con, ok := dbCon.(*sqlx.DB)
	if !ok {
		return nil, fmt.Errorf("return type (%T) not matched, expect *sqlx.DB", dbCon)
	}
	if con == nil {
		return nil, fmt.Errorf("return nil")
	}

	return con, nil
}

func dbOpenImplement(database *Database, create bool) (*sqlx.DB, error) {
	switch database.Driver {
	case DriverMysql:
		return database.OpenMysql(create)
	case DriverSQLite, DriverSQLite3:
		return openSQLite3(database, create)
	default:
		return nil, fmt.Errorf("not implement type: %s", database.Driver)
	}
}

// openSQLite3 open implement for SQLite db
// 	d.DB 作为 文件名 basename
// 	d.Host 作为 文件的目录路径
func openSQLite3(d *Database, create bool) (*sqlx.DB, error) {
	if d == nil {
		return sqlx.Open(d.Driver, SQLiteMemroy)
	}
	if d.User != "" {
		return nil, fmt.Errorf("not support user auth yet")
	}
	file := d.DB
	if d.Host != "" {
		file = filepath.Join(d.Host, file)
	}
	if create {
		if err := fileCreateIfNotExist(file); err != nil {
			return nil, err
		}
	}

	dataSource := fmt.Sprintf("file:%s?cache=shared", file)
	return sqlx.Open(d.Driver, dataSource)
}

func fileCreateIfNotExist(file string) error {
	_, err := os.Stat(file)
	if err == nil || !os.IsNotExist(err) {
		return err
	}

	_, err = os.Create(file)
	return err
}
