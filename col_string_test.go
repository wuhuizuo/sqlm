package sqlm

import (
	"database/sql/driver"
	"reflect"
	"testing"
)

func TestStringColScan(t *testing.T) {
	type args struct {
		val interface{}
	}

	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			"valid bytes value",
			args{val: []byte("123")},
			"123",
			false,
		},
		{
			"empty bytes value",
			args{val: []byte{}},
			"",
			false,
		},
		{
			"valid string value",
			args{val: "{}"},
			"{}",
			false,
		},
		{
			"empty string value",
			args{val: ""},
			"",
			false,
		},
		{
			"other type value",
			args{val: 123},
			"",
			true,
		},
		{
			"nil value",
			args{val: nil},
			"",
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := StringColScan(tt.args.val)
			if (err != nil) != tt.wantErr {
				t.Errorf("StringColScan() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("StringColScan() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNullString_Scan(t *testing.T) {
	type args struct {
		val interface{}
	}

	tests := []struct {
		name    string
		c       *NullString
		args    args
		wantErr bool
	}{
		{
			"empty string",
			new(NullString),
			args{val: ""},
			false,
		},
		{
			"empty bytes",
			new(NullString),
			args{val: []byte{}},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.c.Scan(tt.args.val); (err != nil) != tt.wantErr {
				t.Errorf("NullString.Scan() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNullString_Value(t *testing.T) {
	tests := []struct {
		name    string
		c       NullString
		want    driver.Value
		wantErr bool
	}{
		{
			"with content",
			NullString("123"),
			"123",
			false,
		},
		{
			"empty",
			NullString(""),
			"",
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.c.Value()
			if (err != nil) != tt.wantErr {
				t.Errorf("NullString.Value() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NullString.Value() = %v, want %v", got, tt.want)
			}
		})
	}
}
