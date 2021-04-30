package sqlm

import (
	"crypto/md5"
	"database/sql/driver"
	"fmt"
	"reflect"
	"testing"
)

func TestJSONColScan(t *testing.T) {
	type args struct {
		val  interface{}
		dest interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			"valid bytes value",
			args{val: []byte("{}"), dest: new(map[string]string)},
			false,
		},
		{
			"invalid bytes value",
			args{val: []byte("[]"), dest: new(map[string]string)},
			true,
		},
		{
			"valid string value",
			args{val: "{}", dest: new(map[string]string)},
			false,
		},
		{
			"invalid string value",
			args{val: "[]", dest: new(map[string]string)},
			true,
		},
		{
			"other type value",
			args{val: '{', dest: new(map[string]string)},
			true,
		},
		{
			"nil value",
			args{val: nil, dest: new(map[string]string)},
			false,
		},
		{
			"nil dest",
			args{val: "{}", dest: nil},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := JSONColScan(tt.args.val, tt.args.dest); (err != nil) != tt.wantErr {
				t.Errorf("JSONColScan() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHashCol_MD5(t *testing.T) {
	tests := []struct {
		name string
		c    *HashCol
		want string
	}{
		{
			"nil value",
			new(HashCol),
			fmt.Sprintf("%x", md5.Sum([]byte("null"))),
		},
		{
			"empty",
			&HashCol{},
			fmt.Sprintf("%x", md5.Sum([]byte("{}"))),
		},
		{
			"occur error when marshal",
			&HashCol{"a": make(chan int)},
			"",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.c.MD5(); got != tt.want {
				t.Errorf("HashCol.MD5() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStringList_Scan(t *testing.T) {
	type args struct {
		val interface{}
	}
	tests := []struct {
		name    string
		c       *StringList
		args    args
		wantErr bool
	}{
		{
			"ok",
			new(StringList),
			args{val: "[]"},
			false,
		},
		{
			"invalid value",
			new(StringList),
			args{val: "{}"},
			true,
		},
		{
			"nil",
			nil,
			args{val: "[]"},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.c.Scan(tt.args.val); (err != nil) != tt.wantErr {
				t.Errorf("StringList.Scan() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestStringList_Value(t *testing.T) {
	tests := []struct {
		name    string
		c       StringList
		want    driver.Value
		wantErr bool
	}{
		{
			"empty nil",
			nil,
			[]byte(`null`),
			false,
		},
		{
			"empty but not nil",
			StringList{},
			[]byte(`[]`),
			false,
		},
		{
			"with elements",
			StringList{"123", "456"},
			[]byte(`["123","456"]`),
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.c.Value()
			if (err != nil) != tt.wantErr {
				t.Errorf("StringList.Value() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("StringList.Value() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestValList_Scan(t *testing.T) {
	type args struct {
		val interface{}
	}
	tests := []struct {
		name    string
		c       *ValList
		args    args
		wantErr bool
	}{
		{
			"ok",
			new(ValList),
			args{val: "[]"},
			false,
		},
		{
			"invalid value",
			new(ValList),
			args{val: "{}"},
			true,
		},
		{
			"nil",
			nil,
			args{val: "[]"},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.c.Scan(tt.args.val); (err != nil) != tt.wantErr {
				t.Errorf("ValList.Scan() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValList_Value(t *testing.T) {
	tests := []struct {
		name    string
		c       ValList
		want    driver.Value
		wantErr bool
	}{
		{
			"empty nil",
			nil,
			[]byte(`null`),
			false,
		},
		{
			"empty but not nil",
			ValList{},
			[]byte(`[]`),
			false,
		},
		{
			"with elements",
			ValList{"123", 456},
			[]byte(`["123",456]`),
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.c.Value()
			if (err != nil) != tt.wantErr {
				t.Errorf("ValList.Value() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ValList.Value() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestURLQueryValues_Scan(t *testing.T) {
	type args struct {
		val interface{}
	}
	tests := []struct {
		name    string
		c       *URLQueryValues
		args    args
		wantErr bool
	}{
		{
			"ok",
			new(URLQueryValues),
			args{val: "{}"},
			false,
		},
		{
			"invalid value",
			new(URLQueryValues),
			args{val: "[]"},
			true,
		},
		{
			"nil",
			nil,
			args{val: "{}"},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.c.Scan(tt.args.val); (err != nil) != tt.wantErr {
				t.Errorf("URLQueryValues.Scan() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestURLQueryValues_Value(t *testing.T) {
	tests := []struct {
		name    string
		c       URLQueryValues
		want    driver.Value
		wantErr bool
	}{
		{
			"empty nil",
			nil,
			[]byte(`null`),
			false,
		},
		{
			"empty but not nil",
			URLQueryValues{},
			[]byte(`{}`),
			false,
		},
		{
			"with elements",
			URLQueryValues{"xxx": []string{"123", "456"}},
			[]byte(`{"xxx":["123","456"]}`),
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.c.Value()
			if (err != nil) != tt.wantErr {
				t.Errorf("URLQueryValues.Value() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("URLQueryValues.Value() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHashCol_Scan(t *testing.T) {
	type args struct {
		val interface{}
	}
	tests := []struct {
		name    string
		c       *HashCol
		args    args
		wantErr bool
	}{
		{
			"ok",
			new(HashCol),
			args{val: "{}"},
			false,
		},
		{
			"invalid value",
			new(HashCol),
			args{val: "[]"},
			true,
		},
		{
			"nil",
			nil,
			args{val: "{}"},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.c.Scan(tt.args.val); (err != nil) != tt.wantErr {
				t.Errorf("HashCol.Scan() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHashCol_Value(t *testing.T) {
	tests := []struct {
		name    string
		c       HashCol
		want    driver.Value
		wantErr bool
	}{
		{
			"empty nil",
			nil,
			[]byte(`null`),
			false,
		},
		{
			"empty but not nil",
			HashCol{},
			[]byte(`{}`),
			false,
		},
		{
			"with elements",
			HashCol{"a": "123", "b": 456},
			[]byte(`{"a":"123","b":456}`),
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.c.Value()
			if (err != nil) != tt.wantErr {
				t.Errorf("HashCol.Value() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("HashCol.Value() = %v, want %v", got, tt.want)
			}
		})
	}
}
