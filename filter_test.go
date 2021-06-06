package sqlm

import (
	"reflect"
	"testing"
)

type xxxStruct struct {
	L [3]int64 `db:"l" json:"l,omitempty"`
}

type testStruct struct {
	A string            `db:"a"`
	B int               `db:"b"`
	C []byte            `db:"c"`
	D bool              `db:"d"`
	E float32           `db:"e"`
	F uint              `db:"f"`
	G *uint             `db:"g"`
	H interface{}       `db:"h"`
	I []int32           `db:"i"`
	J []int16           `db:"j"`
	K []byte            `db:"k"`
	L [3]int64          `db:"l"`
	M map[string]string `db:"m"`
	N xxxStruct         `db:"n"`
	O *xxxStruct        `db:"o"`
	P **xxxStruct       `db:"p"`
	Y []interface{}     `db:"y"`
	Z *string           `db:"-"`
}

func testFilterWherePattern(t *testing.T, name string, input RowFilter, want *SQLWhere, wantErr bool) {
	t.Run(name, func(t *testing.T) {
		got, err := input.WherePattern()
		if (err != nil) != wantErr {
			t.Errorf("RowFilterAnd.WherePattern() error = %v, wantErr %v", err, wantErr)
			return
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("RowFilterAnd.WherePattern() got = %v, want %v", got, want)
		}
	})
}

func TestFilterWherePattern(t *testing.T) {
	tests := []struct {
		name    string
		f       RowFilter
		want    *SQLWhere
		wantErr bool
	}{
		{"SelectorFilter-empty", SelectorFilter{}, nil, false},
		{"SelectorFilter-valid",
			SelectorFilter{"a": 1},
			&SQLWhere{Format: "a=:a", Patterns: SelectorFilter{"a": 1}},
			false},
		{"ColListFilter-empty", ColListFilter{}, nil, true},
		{"ColListFilter-noValues", ColListFilter{Col: "abc"}, nil, true},
		{"ColListFilter-stringList",
			ColListFilter{Col: "abc", Values: []interface{}{"abc", "def"}},
			&SQLWhere{Format: "abc='abc' OR abc='def'"},
			false},
		{"ColListFilter-numberList",
			ColListFilter{Col: "abc", Values: []interface{}{123, 456.123}},
			&SQLWhere{Format: "abc=123 OR abc=456.123"},
			false},
		{"HashColFilter-empty", HashColFilter{}, nil, true},
		{"IDListFilter-empty", IDListFilter{}, nil, true},
		{
			"IDListFilter-valid",
			IDListFilter{123, 456},
			&SQLWhere{Format: "id=123 OR id=456"},
			false,
		},
		{"StructFilter-err",
			StructFilter{
				Cols: []string{
					"y",
				},
				Value: &testStruct{
					Y: []interface{}{make(chan bool)},
				},
			},
			nil,
			true,
		},
	}

	for _, tt := range tests {
		testFilterWherePattern(t, tt.name, tt.f, tt.want, tt.wantErr)
	}
}

func TestStructFilter_wherePattern(t *testing.T) {
	tests := []struct {
		name    string
		f       StructFilter
		want    SelectorFilter
		wantErr bool
	}{
		{
			"diff type field",
			StructFilter{
				Cols: []string{
					"a", "b", "c", "d", "e",
					"f", "g", "h", "i", "j",
					"k", "l", "m", "n", "o",
					"p", "not_exist",
				},
				Value: &testStruct{
					A: "123",
					B: 123,
					C: []byte("456"),
					D: true,
					E: 1.00,
					F: 123,
					G: new(uint),
					H: []int{1, 2, 3},
					I: []int32{1, 2, 3},
					J: []int16{},
					K: []byte{},
					L: [3]int64{1, 2, 3},
					M: map[string]string{"a": "1", "b": "2"},
					N: xxxStruct{},
					O: &xxxStruct{L: [3]int64{1, 2, 3}},
				},
			},
			SelectorFilter{
				"a": "123",
				"b": int64(123),
				"c": "456",
				"d": true,
				"e": float64(1.00),
				"f": uint64(123),
				"g": uint64(0),
				"h": []int{1, 2, 3},
				"i": `[1,2,3]`,
				"j": `[]`,
				"k": `""`,
				"l": `[1,2,3]`,
				"m": `{"a":"1","b":"2"}`,
				"n": `{"l":[0,0,0]}`,
				"o": `{"l":[1,2,3]}`,
			},
			false,
		},
		{
			"err filter",
			StructFilter{
				Cols: []string{
					"y",
				},
				Value: &testStruct{
					Y: []interface{}{make(chan bool)},
				},
			},
			nil,
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.f.wherePattern()
			if (err != nil) != tt.wantErr {
				t.Errorf("StructFilter.WherePattern() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("StructFilter.WherePattern() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_parseJSONFieldValue(t *testing.T) {
	tests := []struct {
		name    string
		val     reflect.Value
		want    string
		wantErr bool
	}{
		{"error", reflect.ValueOf(make(chan bool)), "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseJSONFieldValue(tt.val)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseJSONFieldValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("parseJSONFieldValue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStructFilter_WherePattern(t *testing.T) {
	type fields struct {
		Cols  []string
		Value interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		want    *SQLWhere
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := StructFilter{
				Cols:  tt.fields.Cols,
				Value: tt.fields.Value,
			}
			got, err := f.WherePattern()
			if (err != nil) != tt.wantErr {
				t.Errorf("StructFilter.WherePattern() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("StructFilter.WherePattern() = %v, want %v", got, tt.want)
			}
		})
	}
}
