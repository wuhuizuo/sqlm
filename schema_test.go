package sqlm

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/ahmetb/go-linq"
)

// nolint: unparam
func newTestTableSchema(driver, name string, model interface{}) *TableSchema {
	s := NewTableSchema(reflect.TypeOf(model))
	s.Driver = driver
	s.Name = name
	return s
}

func TestTableSchemaColNames(t *testing.T) {
	tests := []struct {
		name string
		t    *TableSchema
		want []string
	}{
		{
			"RecordTest",
			newTestTableSchema(DriverMysql, "xxx", RecordTest{}),
			[]string{
				"id",
				"projectId",
				"ruleId",
				"sendStatus",
				"ensureStatus",
				"ensureUser",
				"ensureTime",
				"createtime",
				"title",
				"body",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.t.ColNames(true); !reflect.DeepEqual(
				linq.From(got).OrderBy(func(e interface{}) interface{} { return e }).Results(),
				linq.From(tt.want).OrderBy(func(e interface{}) interface{} { return e }).Results(),
			) {
				t.Errorf("TableSchema.ColNames() = \n%v, \nwant \n%v", got, tt.want)
			}
		})
	}
}

func TestTableSchemaTargetName(t *testing.T) {
	tests := []struct {
		name    string
		t       *TableSchema
		by      interface{}
		want    string
		wantErr bool
	}{
		{
			"by struct value",
			newTestTableSchema(DriverMysql, "xxx", RecordTest{}),
			RecordTest{},
			"xxx_0",
			false,
		},
		{
			"by struct ptr",
			newTestTableSchema(DriverMysql, "xxx", RecordTest{}),
			&RecordTest{},
			"xxx_0",
			false,
		},
		{
			"by empty filter/struct ptr",
			newTestTableSchema(DriverMysql, "xxx", RecordTest{}),
			nil,
			"xxx",
			false,
		},
		{
			"by filter but lacking the split column pattern",
			newTestTableSchema(DriverMysql, "xxx", RecordTest{}),
			SelectorFilter{"ruleId": 123},
			"",
			true,
		},
		{
			"by not filter or struct ptr",
			newTestTableSchema(DriverMysql, "xxx", RecordTest{}),
			map[string]interface{}{"ruleId": 123},
			"",
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.t.TargetName(tt.by)
			if (err != nil) != tt.wantErr {
				t.Errorf("TableSchema.TargetName() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("TableSchema.TargetName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTableSchemaCreateSQL(t *testing.T) {
	tableName := "test"
	tests := []struct {
		name          string
		columns       []ColSchema
		driverWantMap map[string]string
	}{
		{
			"no_cols",
			nil,
			map[string]string{
				DriverMysql:  fmt.Sprintf(tableCreateSQLTpl, tableName, ""),
				DriverSQLite: fmt.Sprintf(tableCreateSQLTpl, tableName, ""),
			},
		},
		{
			"no_primary",
			[]ColSchema{{Name: "a", JSONName: "a", Type: "varchar(32)"}},
			map[string]string{
				DriverMysql: fmt.Sprintf(tableCreateSQLTpl, tableName, strings.Join([]string{
					"a varchar(32)",
				}, ",\n")),
				DriverSQLite: fmt.Sprintf(tableCreateSQLTpl, tableName, strings.Join([]string{
					"a varchar(32)",
				}, ",\n")),
			},
		},
		{
			"primary",
			[]ColSchema{{Name: "a", JSONName: "a", Type: "varchar(32)", Primary: true}},
			map[string]string{
				DriverMysql: fmt.Sprintf(tableCreateSQLTpl, tableName, strings.Join([]string{
					"a varchar(32) PRIMARY KEY",
				}, ",\n")),
				DriverSQLite: fmt.Sprintf(tableCreateSQLTpl, tableName, strings.Join([]string{
					"a varchar(32) PRIMARY KEY",
				}, ",\n")),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k, want := range tt.driverWantMap {
				s := &TableSchema{
					Driver:  k,
					Name:    tableName,
					Columns: tt.columns,
				}
				if got := s.CreateSQL(); got != want {
					t.Errorf("TableSchema.CreateSQL() = %v, want %v", got, want)
				}
			}
		})
	}
}
