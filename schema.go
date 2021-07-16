package sqlm

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/ahmetb/go-linq"
	"github.com/jmoiron/sqlx/reflectx"
)

const (
	// DBSchemaTag struct field tag for table column schema
	DBSchemaTag = "db"
	// DBJsonTag struct field tag for marshal to json
	DBJsonTag = "json"

	// DBKeyNotNull for col schema key: not_null
	DBKeyNotNull = "not_null"
	// DBKeyPrimary for col schema key: primary
	DBKeyPrimary = "primary"
	// DBKeyDefault for col schema key: default
	DBKeyDefault = "default"
	// DBKeyType for col schema key: type
	DBKeyType = "type"
	// DBKeyKey for col schema key: key
	DBKeyKey = "key"
	// DBKeyAutoIncrement for col schema key: auto_increment
	DBKeyAutoIncrement = "auto_increment"
	// DBKeyNotInsert for col schema key: not_insert
	DBKeyNotInsert = "not_insert"
	// DBKeyNotUpdate for col schema key: not_update
	DBKeyNotUpdate = "not_update"
	// DBKeyOnUpdate for col schema key: on_update => ON UPDATE
	DBKeyOnUpdate = "on_update"
	// DBKeyComplex 标识复杂列,复杂列在输出简要记录信息时并不展示
	DBKeyComplex = "complex"
	// DBKeySplit split table by column's value, usually value range is limited.
	DBKeySplit = "split"
	// True for col schema val: true
	True = "true"
	// False for col schema val: false
	False = "false"
	// DateTimeType for db datetime type
	DateTimeType = "DATETIME"
	// DBFnCurrentTimestamp == CURRENT_TIMESTAMP
	DBFnCurrentTimestamp = "CURRENT_TIMESTAMP"

	// AttrOnUpdateMySQL `on update` attribute for mysql table DDL
	AttrOnUpdateMySQL = "ON UPDATE"
	// AttrNotNullMySQL `not null` attribute for mysql table DDL
	AttrNotNullMySQL = "NOT NULL"
	// AttrNotNullSQLite `not null` attribute for sqlite table DDL
	AttrNotNullSQLite = "NOT NULL"
	// AttrDefaultMySQL `default` attribute for mysql table DDL
	AttrDefaultMySQL = "DEFAULT"
	// AttrDefaultSQLite `default` attribute for sqlite table DDL
	AttrDefaultSQLite = "DEFAULT"

	// DriverMysql mysql driver type
	DriverMysql = "mysql"
	// DriverSQLite sqlite driver type
	DriverSQLite = "sqlite"
	// DriverSQLite3 sqlite driver type limit sqlite version: v3+
	DriverSQLite3 = "sqlite3"

	// singlePKCount primary keys count for single primary key
	singlePKCount = 1
	// attrSinglePK primary attr for single primary key
	attrSinglePK = "PRIMARY KEY"

	// emptyStrColAsignPart 数据表字符串列赋值为空值的表示
	emptyStrColAsignPart = "''"
)

// tableCreateSQLTpl 创建表sql语句格式化模板
const tableCreateSQLTpl = "CREATE TABLE IF NOT EXISTS %s (\n%s\n)"

// ColSchema for table column
type ColSchema struct {
	Name          string
	JSONName      string
	Type          string
	DefaultStr    string
	AutoUpdateStr string
	Default       bool
	Key           bool
	NotNull       bool
	NotInsert     bool
	NotUpdate     bool
	AutoUpdate    bool
	Primary       bool
	AutoIncrement bool
	Complex       bool
	Split         bool
}

func (c *ColSchema) colSchemaSQLite(onlyOnePrimaryCol bool) string {
	line := fmt.Sprintf("%s %s", c.Name, c.Type)
	if c.NotNull {
		line += fmt.Sprintf(" %s", AttrNotNullSQLite)
	}
	if c.Default && c.DefaultStr != "" {
		line += fmt.Sprintf(" %s %s", AttrDefaultSQLite, c.DefaultStr)
	}
	if onlyOnePrimaryCol && c.Primary {
		line += fmt.Sprintf(" %s", attrSinglePK)
	}

	return line
}

func (c *ColSchema) colSchemaMysql(autoIncrementExp string, onlyOnePrimaryCol bool) string {
	line := fmt.Sprintf("%s %s", c.Name, c.Type)

	if c.NotNull {
		line += fmt.Sprintf(" %s", AttrNotNullMySQL)
	}
	if c.Default && c.DefaultStr != "" {
		line += fmt.Sprintf(" %s %s", AttrDefaultMySQL, c.DefaultStr)
	}
	if c.AutoUpdate && c.AutoUpdateStr != "" {
		line += fmt.Sprintf(" %s %s", AttrOnUpdateMySQL, c.AutoUpdateStr)
	}
	if onlyOnePrimaryCol && c.Primary {
		line += fmt.Sprintf(" %s", attrSinglePK)
	}
	if c.AutoIncrement {
		line += autoIncrementExp
	}

	return line
}

// TableSchema for table
type TableSchema struct {
	Driver         string
	Name           string
	Columns        []ColSchema
	splitByColumns []string
}

// NewTableSchema from record type
func NewTableSchema(t reflect.Type) *TableSchema {
	s := TableSchema{Columns: colSchemas(t)}
	linq.From(s.Columns).
		Where(func(c interface{}) bool { return c.(ColSchema).Split }).
		Select(func(sc interface{}) interface{} { return sc.(ColSchema).Name }).
		OrderBy(func(n interface{}) interface{} { return n }).
		ToSlice(&s.splitByColumns)

	return &s
}

// TargetName get the real table name to inserting/query/updating/deleting
func (t *TableSchema) TargetName(by interface{}) (string, error) {
	if len(t.splitByColumns) == 0 || by == nil {
		return t.Name, nil
	}

	if f, ok := by.(RowFilter); ok { // by filter: update/deleting/query
		return t.TargetNameWithFilter(f)
	}

	switch reflect.ValueOf(by).Kind() {
	case reflect.Struct:
		return t.TargetNameWithFilter(&StructFilter{Cols: t.splitByColumns, Value: by})
	case reflect.Interface, reflect.Ptr:
		if reflect.ValueOf(by).Elem().Kind() == reflect.Struct {
			return t.TargetNameWithFilter(&StructFilter{Cols: t.splitByColumns, Value: by})
		}
		return "", fmt.Errorf("no support args for compute target table name: [%T] %v", by, by)
	default:
		return "", fmt.Errorf("no support args for compute target table name: [%T] %v", by, by)
	}
}

// TargetNameWithFilter get the real table name to inserting/query/updating/deleting
func (t *TableSchema) TargetNameWithFilter(filter RowFilter) (string, error) {
	ret := t.Name
	if filter == nil {
		return ret, nil
	}
	if len(t.splitByColumns) == 0 {
		return ret, nil
	}

	where, err := filter.WherePattern()
	if err != nil {
		return "", err
	}
	if where == nil {
		return "", nil
	}

	for _, c := range t.splitByColumns {
		errColMiss := fmt.Errorf("col %s is required in where patterns for compute target table name", c)
		if len(where.Patterns) == 0 {
			return "", errColMiss
		}
		v, ok := where.Patterns[c]
		if !ok {
			return "", errColMiss
		}

		switch v.(type) {
		case float32, float64:
			return "", fmt.Errorf("col %s for compute target table name should not to be float", c)
		case []byte:
			return "", fmt.Errorf("col %s for compute target table name should not to be []byte", c)
		default:
			ret = fmt.Sprintf("%s_%v", ret, v)
		}
	}

	return ret, nil
}

// ColNames list columns for record store/list
func (t *TableSchema) ColNames(all bool) []string {
	var cols []string
	for _, c := range t.Columns {
		if all || !c.Complex {
			cols = append(cols, c.Name)
		}
	}
	return cols
}

// ComplexColNames list complex columns for list
func (t *TableSchema) ComplexColNames() []string {
	var cols []string
	for _, c := range t.Columns {
		if c.Complex {
			cols = append(cols, c.Name)
		}
	}
	return cols
}

// KeyCol return key col name for table
func (t *TableSchema) KeyCol() string {
	for _, c := range t.Columns {
		if c.AutoIncrement || c.Key {
			return c.Name
		}
	}
	return ""
}

// PrimaryCols return primary cols for table
func (t *TableSchema) PrimaryCols() []string {
	var ret []string
	for _, c := range t.Columns {
		if c.Primary {
			ret = append(ret, c.Name)
		}
	}
	return ret
}

// InsertCols list all columns that should fill when inserting
func (t *TableSchema) InsertCols() []string {
	var ret []string
	for _, c := range t.Columns {
		if !c.NotInsert && !c.AutoIncrement {
			ret = append(ret, c.Name)
		}
	}
	return ret
}

// UpdateCols for update exist record
func (t *TableSchema) UpdateCols() []string {
	var ret []string
	for _, c := range t.Columns {
		shouldUpdate := !c.AutoIncrement && !c.NotUpdate && !c.AutoUpdate && !c.Split
		if shouldUpdate {
			ret = append(ret, c.Name)
		}
	}
	return ret
}

// UpdateColsWhenDup for insert when exist dup with same primary keys
func (t *TableSchema) UpdateColsWhenDup() []string {
	var ret []string
	for _, c := range t.Columns {
		shouldUpdate := !c.Primary && !c.AutoIncrement && !c.NotUpdate && !c.AutoUpdate && !c.Split
		if shouldUpdate {
			ret = append(ret, c.Name)
		}
	}
	return ret
}

// UpdatePatternsWhenDup return update pattern string
func (t *TableSchema) UpdatePatternsWhenDup() string {
	var updatePatterns []string
	for _, k := range t.UpdateColsWhenDup() {
		updatePatterns = append(updatePatterns, fmt.Sprintf("%s=:%s", k, k))
	}
	return strings.Join(updatePatterns, ",")
}

// schema return table's all columns schema
func (t *TableSchema) schema() string {
	switch t.Driver {
	case DriverMysql:
		return t.schemaMysql()
	case DriverSQLite, DriverSQLite3:
		return t.schemaSQLite()
	default:
		return ""
	}
}

func (t *TableSchema) schemaMysql() string {
	var lines []string
	var onlyOnePrimaryCol bool

	primaryKeys := t.PrimaryCols()
	if len(primaryKeys) == singlePKCount {
		onlyOnePrimaryCol = true
	}
	autoIncrementExp := " " + autoIncrementKey(t.Driver)
	for _, c := range t.Columns {
		lines = append(lines, c.colSchemaMysql(autoIncrementExp, onlyOnePrimaryCol))
	}

	// primary key 和 索引的设置
	if len(primaryKeys) > singlePKCount {
		lines = append(lines, fmt.Sprintf("%s (%s)", attrSinglePK, strings.Join(primaryKeys, ",")))
	}
	if key := t.KeyCol(); key != "" {
		lines = append(lines, fmt.Sprintf("KEY %s (%s)", key, key))
	}

	return strings.Join(lines, ",\n")
}

func (t *TableSchema) schemaSQLite() string {
	// SQLite 不推荐用 AutoIncrement,如果作为主键,会自增长的
	for i, c := range t.Columns {
		if c.AutoIncrement {
			t.Columns[i].Primary = true
		}
	}

	var onlyOnePrimaryCol bool
	primaryKeys := t.PrimaryCols()
	if len(primaryKeys) == singlePKCount {
		onlyOnePrimaryCol = true
	}

	var lines []string
	for _, c := range t.Columns {
		lines = append(lines, c.colSchemaSQLite(onlyOnePrimaryCol))
	}

	if len(primaryKeys) > singlePKCount {
		lines = append(lines, fmt.Sprintf("%s (%s)", attrSinglePK, strings.Join(primaryKeys, ",")))
	}

	return strings.Join(lines, ",\n")
}

// CreateSQL return sql statement for creating table, like:
//   CREATE TABLE people (
// 	   person_id INTEGER PRIMARY key NOTNULL AUTOINCREMENT,
// 	   first_name text NOT NULL,
// 	   last_name text NOT NULL
//   );
func (t *TableSchema) CreateSQL() string {
	schemaSQL := t.schema()
	query := fmt.Sprintf(tableCreateSQLTpl, t.Name, schemaSQL)
	return query
}

// SelectSQL return sql statement for select quering
func (t *TableSchema) SelectSQL(rf RowFilter, options ListOptions) (Query, map[string]interface{}, error) {
	var selectStatement Query

	if !options.AllColumns && len(options.Columns) > 0 {
		selectStatement.Columns = options.Columns
	} else {
		if options.AllColumns {
			selectStatement.Columns = []string{"*"}
		} else {
			selectStatement.Columns = t.ColNames(options.AllColumns)
		}
	}

	// 根据 filter 分表
	targetTable, err := t.TargetName(rf)
	if err != nil {
		return selectStatement, nil, err
	}
	selectStatement.From = targetTable
	selectStatement.Distinct = options.Distinct
	selectStatement.OrderByColumn = options.OrderByColumn
	selectStatement.OrderDesc = options.OrderDesc
	if options.Distinct {
		// 使用distinct了,不能查询 key键
		var newColumns []string
		keyCol := t.KeyCol()
		linq.From(selectStatement.Columns).
			Where(func(e interface{}) bool { return e.(string) != keyCol }).
			ToSlice(&newColumns)
		selectStatement.Columns = newColumns
	}

	if rf == nil {
		return selectStatement, nil, err
	}

	where, err := rf.WherePattern()
	if err != nil {
		return selectStatement, nil, fmt.Errorf("where条件组装失败:%s", err.Error())
	}
	if where == nil {
		return selectStatement, nil, nil
	}

	selectStatement.Where = where.Format
	if where.Join != nil {
		midTableName := "t"
		selectStatement.From = selectStatement.From + " " + midTableName
		selectStatement.Where = strings.Replace(selectStatement.Where, where.Join.OriginTablePlaceholder, t.Name, -1)
		selectStatement.Where = strings.Replace(selectStatement.Where, where.Join.TempTablePlaceholder, midTableName, -1)
	}

	return selectStatement, where.Patterns, err
}

// UniqWhereFormatter uniq record select filter
func UniqWhereFormatter(t *Table) string {
	var whereFormater []string
	for _, k := range t.Schema().PrimaryCols() {
		whereFormater = append(whereFormater, fmt.Sprintf("%s=:%s", k, k))
	}
	return strings.Join(whereFormater, " AND ")
}

func colSchemaDefault(f *reflectx.FieldInfo) (defaultOn bool, defaultVal string) {
	return colSchemaValSetter(f, DBKeyDefault)
}

func colSchemaOnUpdate(f *reflectx.FieldInfo) (onUpdateEnable bool, updateVal string) {
	onUpdateEnable, updateVal = colSchemaValSetter(f, DBKeyOnUpdate)
	if onUpdateEnable && (updateVal == "" || updateVal == emptyStrColAsignPart) {
		defaultEnable, defaultVal := colSchemaValSetter(f, DBKeyDefault)
		if defaultEnable {
			updateVal = defaultVal
		}
	}

	return
}

func colSchemaValSetter(field *reflectx.FieldInfo, optionKey string) (exist bool, setVal string) {
	v, exist := field.Options[optionKey]
	if !exist {
		return exist, ""
	}

	switch field.Field.Type.Kind() {
	case reflect.String:
		setVal = stringColSchemaValSetter(field, v)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Float32, reflect.Float64, reflect.Bool:
		setVal = numberColSchemaValSetter(v)
	}

	return exist, setVal
}

func numberColSchemaValSetter(value string) string {
	if value == "" {
		return "0"
	}
	return value
}

func stringColSchemaValSetter(field *reflectx.FieldInfo, value string) string {
	setVal := emptyStrColAsignPart
	t := field.Options[DBKeyType]
	switch t {
	case DateTimeType, strings.ToLower(DateTimeType):
		if strings.EqualFold(DBFnCurrentTimestamp, value) {
			setVal = DBFnCurrentTimestamp
		}
	default:
		if value != "" {
			setVal = fmt.Sprintf("'%s'", value)
		}
	}

	return setVal
}

func colSchemas(t reflect.Type) []ColSchema {
	structFieldJSONMap := parseStructJSONMap(t)

	// 解析db相关标记
	var schemas []ColSchema
	fields := reflectx.NewMapper(DBSchemaTag).TypeMap(t).Tree.Children
	for _, f := range fields {
		column := colSchema(f, structFieldJSONMap)
		if column == nil {
			continue
		}
		schemas = append(schemas, *column)
	}

	return schemas
}

// parseStructJSONMap 解析struct json相关映射
func parseStructJSONMap(t reflect.Type) map[string]string {
	structFieldJSONMap := map[string]string{}
	for _, f := range reflectx.NewMapper(DBJsonTag).TypeMap(t).Tree.Children {
		if f == nil {
			continue
		}
		structFieldJSONMap[f.Field.Name] = f.Name
	}

	return structFieldJSONMap
}

// colSchema 解析struct 中单个成员的列 db存储模型
func colSchema(field *reflectx.FieldInfo, structFieldJSONMap map[string]string) *ColSchema {
	if field == nil {
		return nil
	}
	// TODO: db类型在没有显示说明时，能从go类型中自动猜测
	column := ColSchema{Name: field.Name}
	if jv, ok := structFieldJSONMap[field.Field.Name]; ok {
		column.JSONName = jv
	}

	// 名和类型
	if v, ok := field.Options[DBKeyType]; ok {
		column.Type = v
	}

	// 默认值
	column.Default, column.DefaultStr = colSchemaDefault(field)

	// 自动更新字段
	column.AutoUpdate, column.AutoUpdateStr = colSchemaOnUpdate(field)

	// 各种开关属性解析
	switchMap := map[string]*bool{
		DBKeyKey:           &column.Key,
		DBKeyAutoIncrement: &column.AutoIncrement,
		DBKeyPrimary:       &column.Primary,
		DBKeyComplex:       &column.Complex,
		DBKeySplit:         &column.Split,
		DBKeyNotNull:       &column.NotNull,
		DBKeyNotInsert:     &column.NotInsert,
		DBKeyNotUpdate:     &column.NotUpdate,
	}
	for s, p := range switchMap {
		if v, ok := field.Options[s]; ok {
			ret := v == True || v == ""
			*p = ret
		}
	}

	return &column
}

func autoIncrementKey(driver string) string {
	m := map[string]string{
		DriverMysql:   "AUTO_INCREMENT",
		DriverSQLite:  "AUTOINCREMENT",
		DriverSQLite3: "AUTOINCREMENT",
		// Add more...
	}

	v := m[driver]
	return v
}
