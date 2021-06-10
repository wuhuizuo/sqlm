package sqlm

import (
	"reflect"
	"testing"

	"github.com/ahmetb/go-linq"
	"github.com/jmoiron/sqlx"
)

func Test_loadDataForUpdate(t *testing.T) {
	type args struct {
		t    TableFuncInterface
		src  map[string]interface{}
		dest interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    []string
		wantErr bool
	}{
		{
			"RecordTest",
			args{
				&testTable{},
				map[string]interface{}{
					"id":           nil,
					"ruleId":       nil,
					"createtime":   nil,
					"sendStatus":   nil,
					"ensureUser":   nil,
					"ensureStatus": nil,
					"ensureTime":   nil,
					"title":        nil,
					"body":         nil,
				},
				&testRecord{},
			},
			[]string{
				"id",
				"ruleId",
				"createtime",
				"sendStatus",
				"ensureUser",
				"ensureStatus",
				"ensureTime",
				"title",
				"body",
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := loadDataForUpdate(tt.args.t, tt.args.src, tt.args.dest)
			if (err != nil) != tt.wantErr {
				t.Errorf("loadDataForUpdate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			var sortGot []string
			var sortWant []string
			linq.From(got).OrderBy(func(e interface{}) interface{} { return e }).ToSlice(&sortGot)
			linq.From(tt.want).OrderBy(func(e interface{}) interface{} { return e }).ToSlice(&sortWant)
			if !reflect.DeepEqual(sortGot, sortWant) {
				t.Errorf("loadDataForUpdate() = %v, want %v", sortGot, sortWant)
			}
		})
	}
}

func TestScanRow(t *testing.T) {
	type args struct {
		t    TableFuncInterface
		rows *sqlx.Rows
	}

	tests := []struct {
		name    string
		args    args
		want    interface{}
		wantErr bool
	}{
		{"nil rows", args{&testTable{}, nil}, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ScanRow(tt.args.t, tt.args.rows)
			if (err != nil) != tt.wantErr {
				t.Errorf("ScanRow() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ScanRow() = %v, want %v", got, tt.want)
			}
		})
	}
}
