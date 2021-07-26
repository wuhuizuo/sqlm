// Package sqlm 实现数据库对接的抽象封装,考虑性能和自由度要求不用ORM
package sqlm

import (
	"fmt"
	"reflect"
	"testing"
)

func TestDBCreateIterStructField(t *testing.T) {
	fakeServer, err := newFakeMysqlServer()
	if err != nil {
		t.Fatal(err)
	}

	go func() { _ = fakeServer.Start() }()
	defer fakeServer.Close()

	type dbGroupStruct struct {
		Test1 *Table
		Test2 *Table
	}

	t1 := &Table{
		Database: &Database{
			Driver: "mysql",
			DSN:    fmt.Sprintf("user:pass@tcp(%s)/fake", fakeServer.Listener.Addr()),
		},
		TableName: "test_table1",
	}
	t2 := &Table{
		Database: &Database{
			Driver: "mysql",
			DSN:    fmt.Sprintf("user:pass@tcp(%s)/fake", fakeServer.Listener.Addr()),
		},
		TableName: "test_table2",
	}
	dbGroup := dbGroupStruct{
		Test1: t1,
		Test2: t2,
	}
	dbGroup.Test1.SetRowModel(func() interface{} { return &testRecord{} })
	dbGroup.Test2.SetRowModel(func() interface{} { return &testRecord{} })

	t.Run("ok", func(t *testing.T) {
		err := DBCreateIterStructField(reflect.ValueOf(dbGroup), nil)
		if err != nil {
			t.Errorf("DBCreateIterStructField() error = %v, wantErr %v", err, false)
		}
	})
}
