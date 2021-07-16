package sqlm

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/ahmetb/go-linq"
	"github.com/jmoiron/sqlx/reflectx"
)

// SQLWhere present where infos for sql composing.
type SQLWhere struct {
	Format   string                 // 格式化模板
	Patterns map[string]interface{} // 格式化替换值表
	Join     *JoinReplacer          // join 联合查询
}

// RowFilter for compose where statements in query
type RowFilter interface {
	WherePattern() (*SQLWhere, error)
}

// RowFilterAnd for compose filters
type RowFilterAnd []RowFilter

// WherePattern imp for RowFilter interface
func (f RowFilterAnd) WherePattern() (*SQLWhere, error) {
	if len(f) == 0 {
		return nil, nil
	}

	ret := SQLWhere{Patterns: make(map[string]interface{})}

	for _, e := range f {
		if e == nil {
			continue
		}

		eRet, err := e.WherePattern()
		if err != nil {
			return eRet, err
		}
		if eRet == nil {
			continue
		}
		if eRet.Join != nil {
			return nil, fmt.Errorf("not support table join query in filters combining")
		}
		if ret.Format == "" {
			ret.Format = eRet.Format
		} else {
			ret.Format += " AND (" + eRet.Format + ")"
		}
		for k, v := range eRet.Patterns {
			ret.Patterns[k] = v
		}
	}

	return &ret, nil
}

// LikeFilter for like filter
type LikeFilter struct {
	Key   string
	Value string
}

// WherePattern imp for RowFilter interface
func (l LikeFilter) WherePattern() (*SQLWhere, error) {
	if l.Key == "" || l.Value == "" {
		return nil, errors.New("lack key or value")
	}

	format := fmt.Sprintf("%s like :%s", l.Key, l.Key)
	patterns := map[string]interface{}{l.Key: l.Value}
	return &SQLWhere{Format: format, Patterns: patterns}, nil
}

// SelectorFilter simple map selector filter
type SelectorFilter map[string]interface{}

// WherePattern imp for RowFilter interface
func (f SelectorFilter) WherePattern() (*SQLWhere, error) {
	if len(f) == 0 {
		return nil, nil
	}

	var whereFormatter []string
	for k := range f {
		whereFormatter = append(whereFormatter, fmt.Sprintf("%s=:%s", k, k))
	}
	return &SQLWhere{Format: strings.Join(whereFormatter, " AND "), Patterns: f}, nil
}

// TimeFormatFn for time object
type TimeFormatFn func(time.Time) interface{}

// BetweenFilter for col value filter with range
type BetweenFilter struct {
	Col  string
	From interface{}
	To   interface{}
}

// WherePattern imp for RowFilter interface
func (f BetweenFilter) WherePattern() (*SQLWhere, error) {
	if f.Col == "" {
		return nil, errors.New("empty col")
	}
	if f.From == nil {
		return nil, errors.New("invalid range because nil start")
	}
	if f.To == nil {
		return nil, errors.New("invalid range because nil end")
	}

	patterns := make(map[string]interface{})
	fromKey := f.Col + "S"
	toKey := f.Col + "E"
	formatter := fmt.Sprintf("%s BETWEEN :%s AND :%s", f.Col, fromKey, toKey)
	patterns[fromKey] = f.From
	patterns[toKey] = f.To

	return &SQLWhere{Format: formatter, Patterns: patterns}, nil
}

// ColListFilter col value list filter
type ColListFilter struct {
	Col    string
	Values []interface{}
}

// WherePattern return the parts for compose sql update/delete query
func (f ColListFilter) WherePattern() (*SQLWhere, error) {
	return whereListPattern(f.Col, f.Values)
}

// HashColFilter for db json col with hash type
type HashColFilter struct {
	Col   string
	Value HashCol
}

// WherePattern imp for RowFilter interface
func (f HashColFilter) WherePattern() (*SQLWhere, error) {
	if f.Col == "" {
		return nil, fmt.Errorf("should set Col")
	}
	if len(f.Value) == 0 {
		return &SQLWhere{Format: fmt.Sprintf("(%s='%s' OR %s=NULL)", f.Col, "{}", f.Col)}, nil
	}

	var whereFormatter []string
	patterns := map[string]interface{}{}
	for k, v := range f.Value {
		childKey := fmt.Sprintf("JSON_EXTRACT(%s, \"$.%s\")", f.Col, k)
		childPattern := fmt.Sprintf("%s_%s", f.Col, k)
		formatter := fmt.Sprintf("%s=:%s", childKey, childPattern)
		whereFormatter = append(whereFormatter, formatter)
		patterns[childPattern] = v
	}
	return &SQLWhere{Format: strings.Join(whereFormatter, " AND "), Patterns: patterns}, nil
}

// IDListFilter id list filter
type IDListFilter []int32

// WherePattern return the parts for compose sql update/delete query
func (f IDListFilter) WherePattern() (*SQLWhere, error) {
	return whereListPattern("id", linq.From(f).Results())
}

// whereListPattern
func whereListPattern(key string, values []interface{}) (*SQLWhere, error) {
	if key == "" {
		return nil, fmt.Errorf("key filter should set key name")
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("key filter should contain one key val least")
	}

	var whereFormatter []string
	for _, id := range values {
		switch id.(type) {
		case string:
			whereFormatter = append(whereFormatter, fmt.Sprintf("%s='%s'", key, id))
		default:
			whereFormatter = append(whereFormatter, fmt.Sprintf("%s=%v", key, id))
		}
	}

	return &SQLWhere{Format: strings.Join(whereFormatter, " OR ")}, nil
}

// StructFilter 使用结构体作为过滤器
type StructFilter struct {
	Cols  []string    // 使用的标签 `db`
	Value interface{} // 这里需要是一个结构体指针
}

// WherePattern return the parts for compose sql update/delete query
func (f StructFilter) WherePattern() (*SQLWhere, error) {
	filter, err := f.transFilter()
	if err != nil {
		return nil, err
	}

	return filter.WherePattern()
}

func (f StructFilter) transFilter() (SelectorFilter, error) {
	fieldInfos := reflectx.NewMapper(DBSchemaTag).FieldMap(reflect.ValueOf(f.Value))

	selectorFilter := SelectorFilter{}
	for _, k := range f.Cols {
		v, ok := fieldInfos[k]
		if !ok {
			continue
		}

		value, err := parseFieldValue(v)
		if err != nil {
			return nil, err
		}
		if value != nil {
			selectorFilter[k] = value
		}
	}

	return selectorFilter, nil
}

func parseFieldValue(fieldValue reflect.Value) (ret interface{}, err error) {
	val := reflect.Indirect(fieldValue)

	switch val.Kind() {
	case reflect.String:
		ret = val.String()
	case reflect.Bool:
		ret = val.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		ret = val.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Uintptr:
		ret = val.Uint()
	case reflect.Interface:
		ret = val.Interface()
	case reflect.Float32, reflect.Float64:
		ret = val.Float()
	case reflect.Slice, reflect.Array, reflect.Map, reflect.Struct:
		ret, err = parseJSONFieldValue(val)
	default:
		// Chan
		// Func
		// Ptr
	}

	return ret, err
}

func parseJSONFieldValue(val reflect.Value) (string, error) {
	if val.Kind() == reflect.Slice && val.Len() > 0 && val.Index(0).Kind() == reflect.Uint8 {
		return string(val.Bytes()), nil
	}

	bs, err := json.Marshal(val.Interface())
	if err != nil {
		return "", err
	}

	return string(bs), nil
}
