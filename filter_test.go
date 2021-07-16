package sqlm

import (
	"errors"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
)

//go:generate mockgen -package=sqlm -destination=./filter_mock.go github.com/wuhuizuo/sqlm RowFilter

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
		{
			"BetweenFilter-ok",
			BetweenFilter{Col: "a", From: 123, To: 456},
			&SQLWhere{
				Format:   "a BETWEEN :aS AND :aE",
				Patterns: map[string]interface{}{"aS": 123, "aE": 456},
			},
			false,
		},
		{"BetweenFilter-empty-col", BetweenFilter{From: 123, To: 456}, nil, true},
		{"BetweenFilter-nil-from", BetweenFilter{Col: "a", To: 456}, nil, true},
		{"BetweenFilter-nil-to", BetweenFilter{Col: "a", From: 123}, nil, true},
		{"SelectorFilter-empty", SelectorFilter{}, nil, false},
		{
			"SelectorFilter-valid",
			SelectorFilter{"a": 1},
			&SQLWhere{
				Format:   "a=:a",
				Patterns: SelectorFilter{"a": 1},
			},
			false,
		},
		{"ColListFilter-empty", ColListFilter{}, nil, true},
		{"ColListFilter-noValues", ColListFilter{Col: "abc"}, nil, true},
		{
			"ColListFilter-stringList",
			ColListFilter{Col: "abc", Values: []interface{}{"abc", "def"}},
			&SQLWhere{Format: "abc='abc' OR abc='def'"},
			false,
		},
		{
			"ColListFilter-numberList",
			ColListFilter{Col: "abc", Values: []interface{}{123, 456.123}},
			&SQLWhere{Format: "abc=123 OR abc=456.123"},
			false,
		},
		{"HashColFilter-empty", HashColFilter{}, nil, true},
		{
			"HashColFilter-value-nil",
			HashColFilter{Col: "a", Value: nil},
			&SQLWhere{Format: `(a='{}' OR a=NULL)`},
			false,
		},
		{
			"HashColFilter-filled",
			HashColFilter{Col: "h", Value: HashCol{"a": 123}},
			&SQLWhere{
				Format:   `JSON_EXTRACT(h, "$.a")=:h_a`,
				Patterns: map[string]interface{}{"h_a": 123},
			},
			false,
		},
		{"IDListFilter-empty", IDListFilter{}, nil, true},
		{
			"IDListFilter-valid",
			IDListFilter{123, 456},
			&SQLWhere{Format: "id=123 OR id=456"},
			false,
		},
		{
			"StructFilter-err",
			StructFilter{
				Cols:  []string{"y"},
				Value: &testStruct{Y: []interface{}{make(chan bool)}},
			},
			nil,
			true,
		},
		{
			"LikeFilter-ok",
			LikeFilter{Key: "a", Value: "%abcd_"},
			&SQLWhere{
				Format:   `a like :a`,
				Patterns: map[string]interface{}{"a": "%abcd_"},
			},
			false,
		},
		{"LikeFilter-empty-key", LikeFilter{Value: "%abcd_"}, nil, true},
		{"LikeFilter-empty-value", LikeFilter{Key: "a"}, nil, true},
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
			got, err := tt.f.transFilter()
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

func TestRowFilterAnd_WherePattern(t *testing.T) {
	mock := gomock.NewController(t)
	defer mock.Finish()

	joinRowFilter := NewMockRowFilter(mock)
	joinRowFilter.EXPECT().WherePattern().AnyTimes().
		Return(&SQLWhere{
			Format:   "j=:j",
			Patterns: map[string]interface{}{"j": 123},
			Join: &JoinReplacer{
				Join:                   true,
				OriginTablePlaceholder: "oooo",
				TempTablePlaceholder:   "tttt",
			},
		}, nil)

	errRowFilter := NewMockRowFilter(mock)
	errRowFilter.EXPECT().WherePattern().AnyTimes().
		Return(nil, errors.New("mock error"))

	tests := []struct {
		name    string
		f       RowFilterAnd
		want    *SQLWhere
		wantErr bool
	}{
		{"empty list", nil, nil, false},
		{
			"one element",
			RowFilterAnd{SelectorFilter{"a": 123}},
			&SQLWhere{
				Format:   "a=:a",
				Patterns: map[string]interface{}{"a": 123},
			},
			false,
		},
		{
			"with empty elments",
			RowFilterAnd{SelectorFilter{"a": 123}, SelectorFilter{}},
			&SQLWhere{
				Format:   "a=:a",
				Patterns: map[string]interface{}{"a": 123},
			},
			false,
		},
		{
			"with nil elments",
			RowFilterAnd{SelectorFilter{"a": 123}, nil},
			&SQLWhere{
				Format:   "a=:a",
				Patterns: map[string]interface{}{"a": 123},
			},
			false,
		},
		{
			"with two filled elments",
			RowFilterAnd{SelectorFilter{"a": 123}, SelectorFilter{"b": 456}},
			&SQLWhere{
				Format:   "a=:a AND (b=:b)",
				Patterns: map[string]interface{}{"a": 123, "b": 456},
			},
			false,
		},
		{
			"with join elements",
			RowFilterAnd{SelectorFilter{"a": 123}, joinRowFilter},
			nil,
			true,
		},
		{
			"with join elements",
			RowFilterAnd{SelectorFilter{"a": 123}, errRowFilter},
			nil,
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.f.WherePattern()
			if (err != nil) != tt.wantErr {
				t.Errorf("RowFilterAnd.WherePattern() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RowFilterAnd.WherePattern() = %v, want %v", got, tt.want)
			}
		})
	}
}
