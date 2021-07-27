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
			newTestTableSchema(DriverMysql, "xxx", testRecord{}),
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
			newTestTableSchema(DriverMysql, "xxx", testRecord{}),
			testRecord{},
			"xxx_0",
			false,
		},
		{
			"by struct ptr",
			newTestTableSchema(DriverMysql, "xxx", testRecord{}),
			&testRecord{},
			"xxx_0",
			false,
		},
		{
			"by empty filter/struct ptr",
			newTestTableSchema(DriverMysql, "xxx", testRecord{}),
			nil,
			"xxx",
			false,
		},
		{
			"by filter but lacking the split column pattern",
			newTestTableSchema(DriverMysql, "xxx", testRecord{}),
			SelectorFilter{"ruleId": 123},
			"",
			true,
		},
		{
			"by not filter or struct ptr",
			newTestTableSchema(DriverMysql, "xxx", testRecord{}),
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
		name      string
		columns   []*ColSchema
		driver    string
		wantLines []string
	}{
		{
			name:      "no_cols - mysql",
			columns:   nil,
			driver:    DriverMysql,
			wantLines: nil,
		},
		{
			name:      "no_cols - sqlite",
			columns:   nil,
			driver:    DriverSQLite,
			wantLines: nil,
		},
		{
			name:    "no_primary - mysql",
			columns: []*ColSchema{{Name: "a", JSONName: "a", Type: "varchar(32)"}},
			driver:  DriverMysql,
			wantLines: []string{
				"a varchar(32)",
			},
		},
		{
			name:    "no_primary - sqlite",
			columns: []*ColSchema{{Name: "a", JSONName: "a", Type: "varchar(32)"}},

			driver: DriverSQLite,
			wantLines: []string{
				"a varchar(32)",
			},
		},
		{
			name:    "primary - mysql",
			columns: []*ColSchema{{Name: "a", JSONName: "a", Type: "varchar(32)", Primary: true}},
			driver:  DriverMysql,
			wantLines: []string{
				"a varchar(32) PRIMARY KEY",
			},
		},
		{
			name:    "primary - sqlite",
			columns: []*ColSchema{{Name: "a", JSONName: "a", Type: "varchar(32)", Primary: true}},
			driver:  DriverSQLite,
			wantLines: []string{
				"a varchar(32) PRIMARY KEY",
			},
		},
		{
			name: "multi primary key member - mysql",
			columns: []*ColSchema{
				{Name: "id", JSONName: "id", Type: "INT", AutoIncrement: true},
				{Name: "a", JSONName: "a", Type: "varchar(32)", Primary: true},
				{Name: "b", JSONName: "b", Type: "varchar(32)", Primary: true},
			},
			driver: DriverMysql,
			wantLines: []string{
				"id INT NOT NULL AUTO_INCREMENT",
				"a varchar(32)",
				"b varchar(32)",
				"PRIMARY KEY (a,b)",
				"KEY id (id)",
			},
		},
		{
			name:    "auto increment without primary setted - mysql",
			columns: []*ColSchema{{Name: "id", JSONName: "id", Type: "INT", AutoIncrement: true}},
			driver:  DriverMysql,
			wantLines: []string{
				"id INT NOT NULL PRIMARY KEY AUTO_INCREMENT",
			},
		},
		{
			name:    "auto increment without primary setted - sqlite",
			columns: []*ColSchema{{Name: "id", JSONName: "id", Type: "INT", AutoIncrement: true}},
			driver:  DriverSQLite,
			wantLines: []string{
				"id INTEGER PRIMARY KEY",
			},
		},
		{
			name:    "unique key - mysql",
			columns: []*ColSchema{{Name: "id", JSONName: "id", Type: "INT", Unique: true}},
			driver:  DriverMysql,
			wantLines: []string{
				"id INT UNIQUE KEY",
			},
		},
		{
			name:    "unique key - sqlite",
			columns: []*ColSchema{{Name: "id", JSONName: "id", Type: "INT", Unique: true}},
			driver:  DriverSQLite,
			wantLines: []string{
				"id INT UNIQUE",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &TableSchema{
				Driver:  tt.driver,
				Name:    tableName,
				Columns: tt.columns,
			}
			want := fmt.Sprintf(tableCreateSQLTpl, tableName, strings.Join(tt.wantLines, ",\n"))

			if got := s.CreateSQL(); got != want {
				t.Errorf("TableSchema.CreateSQL() = %v, want %v", got, want)
			}
		})
	}
}
