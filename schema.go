package sqlm

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/ahmetb/go-linq"
	"github.com/jmoiron/sqlx/reflectx"
)

// SQL divers.
const (
	DriverMysql   = "mysql"   // DriverMysql mysql driver type
	DriverSQLite  = "sqlite"  // DriverSQLite sqlite driver type
	DriverSQLite3 = "sqlite3" // DriverSQLite3 sqlite driver type limit sqlite version: v3+
)

// record model struct tags.
const (
	DBSchemaTag = "db"   // struct field tag for table column schema.
	DBJsonTag   = "json" //  struct field tag for marshal to json.
)

// schema keys.
const (
	DBKeyNotNull       = "not_null"       // for col schema key: not_null.
	DBKeyPrimary       = "primary"        //  for col schema key: primary.
	DBKeyUnique        = "unique"         // for col schema key: uniq.
	DBKeyDefault       = "default"        // for col schema key: default.
	DBKeyType          = "type"           // for col schema key: type.
	DBKeyKey           = "key"            // for col schema key: key.
	DBKeyAutoIncrement = "auto_increment" // for col schema key: auto_increment.
	DBKeyNotInsert     = "not_insert"     // for col schema key: not_insert.
	DBKeyNotUpdate     = "not_update"     // for col schema key: not_update.
	DBKeyOnUpdate      = "on_update"      // for col schema key: on_update => ON UPDATE.
	DBKeyComplex       = "complex"        // the column should returned zero when simple list.
	DBKeySplit         = "split"          // split table by column's value, usually value range is limited.
)

// SQL keywords.
const (
	AttrOnUpdateMySQL = "ON UPDATE" // AttrOnUpdateMySQL `on update` attribute for mysql table DDL.
	AttrNotNullMySQL  = "NOT NULL"  // AttrNotNullMySQL `not null` attribute for mysql table DDL.
	AttrNotNullSQLite = "NOT NULL"  // AttrNotNullSQLite `not null` attribute for sqlite table DDL.
	AttrDefaultMySQL  = "DEFAULT"   // AttrDefaultMySQL `default` attribute for mysql table DDL.
	AttrDefaultSQLite = "DEFAULT"   // AttrDefaultSQLite `default` attribute for sqlite table DDL.

	DateTimeType         = "DATETIME"          // for db datetime type.
	DBFnCurrentTimestamp = "CURRENT_TIMESTAMP" // is `CURRENT_TIMESTAMP`.
)

// constant values.
const (
	True  = "true"  // True for col schema val: true
	False = "false" // False for col schema val: false
)

const (
	singlePKCount        = 1             // primary keys count for single primary key.
	attrKey              = "KEY"         // common key.
	attrPrimaryKey       = "PRIMARY KEY" // attr for primary key.
	attrUniqueKeyMySQL   = "UNIQUE KEY"  // attr for mysql unique key.
	attrUniqueKeySQLite  = "UNIQUE"      // attr for sqlite unique key.
	emptyStrColAsignPart = "''"          // zero string type column set value.
	tableCreateSQLTpl    = "CREATE TABLE IF NOT EXISTS %s (\n%s\n)"
)

// ColSchema for table column.
type ColSchema struct {
	Name          string
	JSONName      string
	Type          string
	DefaultStr    string
	AutoUpdateStr string
	Default       bool
	NotNull       bool
	NotInsert     bool
	NotUpdate     bool
	AutoUpdate    bool
	Key           bool
	Primary       bool
	Unique        bool
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
		line += fmt.Sprintf(" %s", attrPrimaryKey)
	}
	if c.Unique {
		line += fmt.Sprintf(" %s", attrUniqueKeySQLite)
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
		line += fmt.Sprintf(" %s", attrPrimaryKey)
	}
	if c.Unique {
		line += fmt.Sprintf(" %s", attrUniqueKeyMySQL)
	}
	if c.AutoIncrement {
		line += autoIncrementExp
	}

	return line
}

// set key attr, order is : primary key > unique key > key
func (c *ColSchema) setKeyAttrs() {
	if c.Primary {
		c.Unique = false
		c.Key = false

		// cannot accept NULL values when key is primary.
		c.NotNull = true
		return
	}

	if c.Unique {
		c.Key = false
	}
}

// TableSchema for table
type TableSchema struct {
	Driver         string
	Name           string
	Columns        []*ColSchema
	splitByColumns []string
}

// NewTableSchema from record type
func NewTableSchema(t reflect.Type) *TableSchema {
	s := TableSchema{Columns: colSchemas(t)}
	linq.From(s.Columns).
		Where(func(c interface{}) bool { return c.(*ColSchema).Split }).
		Select(func(sc interface{}) interface{} { return sc.(*ColSchema).Name }).
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
		shouldBeCommonKey := (c.AutoIncrement || c.Key) && (!c.Primary)
		if shouldBeCommonKey {
			return c.Name
		}
	}

	return ""
}

// PrimaryCols return primary cols for table
func (t *TableSchema) PrimaryCols() ([]string, error) {
	var ret []string

	var autoIncrementCol *ColSchema
	for i, c := range t.Columns {
		if c.Primary {
			ret = append(ret, c.Name)
		}
		if c.AutoIncrement {
			autoIncrementCol = t.Columns[i]
		}
	}

	// none explicit primary keys, find auto increase key to set it as primary key.
	if autoIncrementCol == nil {
		return ret, nil
	}

	switch t.Driver {
	case DriverSQLite, DriverSQLite3:
		if len(ret) > 0 {
			return nil, errors.New("sqlite not support both auto increment and other primary columns at same time")
		}
		sqliteAutoIncrementColDeal(autoIncrementCol)
	case DriverMysql:
		if len(ret) > 0 {
			return ret, nil
		}

		mysqlAutoIncrementColDeal(autoIncrementCol)
	default:
		return nil, nil
	}

	return append(ret, autoIncrementCol.Name), nil
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
func (t *TableSchema) schema() (string, error) {
	switch t.Driver {
	case DriverMysql:
		return t.schemaMysql()
	case DriverSQLite, DriverSQLite3:
		return t.schemaSQLite()
	default:
		return "", fmt.Errorf("not support driver: %s", t.Driver)
	}
}

func (t *TableSchema) schemaMysql() (string, error) {
	var onlyOnePrimaryCol bool

	primaryKeys, err := t.PrimaryCols()
	if err != nil {
		return "", err
	}

	if len(primaryKeys) == singlePKCount {
		onlyOnePrimaryCol = true
	}
	autoIncrementExp := " " + autoIncrementKey(t.Driver)

	var lines []string
	for _, c := range t.Columns {
		if c.AutoIncrement {
			c.NotNull = true
		}
		lines = append(lines, c.colSchemaMysql(autoIncrementExp, onlyOnePrimaryCol))
	}

	// primary key 和 索引的设置
	if len(primaryKeys) > singlePKCount {
		lines = append(lines, fmt.Sprintf("%s (%s)", attrPrimaryKey, strings.Join(primaryKeys, ",")))
	}

	if key := t.KeyCol(); key != "" {
		lines = append(lines, fmt.Sprintf("%s %s (%s)", attrKey, key, key))
	}

	return strings.Join(lines, ",\n"), nil
}

func (t *TableSchema) schemaSQLite() (string, error) {
	var onlyOnePrimaryCol bool
	primaryKeys, err := t.PrimaryCols()
	if err != nil {
		return "", nil
	}

	if len(primaryKeys) == singlePKCount {
		onlyOnePrimaryCol = true
	}

	var lines []string
	for _, c := range t.Columns {
		lines = append(lines, c.colSchemaSQLite(onlyOnePrimaryCol))
	}

	if len(primaryKeys) > singlePKCount {
		lines = append(lines, fmt.Sprintf("%s (%s)", attrPrimaryKey, strings.Join(primaryKeys, ",")))
	}

	return strings.Join(lines, ",\n"), nil
}

// CreateSQL return sql statement for creating table, like:
//   CREATE TABLE people (
// 	   person_id INTEGER PRIMARY key NOTNULL AUTOINCREMENT,
// 	   first_name text NOT NULL,
// 	   last_name text NOT NULL
//   );
func (t *TableSchema) CreateSQL() string {
	schemaSQL, err := t.schema()
	if err != nil {
		return ""
	}

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
		return selectStatement, nil, fmt.Errorf("where statement composed failed: %w", err)
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

	pCols, err := t.getSchema().PrimaryCols()
	if err != nil {
		return ""
	}

	for _, k := range pCols {
		whereFormater = append(whereFormater, fmt.Sprintf("%s=:%s", k, k))
	}
	return strings.Join(whereFormater, " AND ")
}

func colSchemaDefault(f *reflectx.FieldInfo) (defaultOn bool, defaultVal string) {
	return colSchemaValSetter(f, DBKeyDefault)
}

func colSchemaOnUpdate(f *reflectx.FieldInfo) (onUpdateEnable bool, updateVal string) {
	onUpdateEnable, updateVal = colSchemaValSetter(f, DBKeyOnUpdate)
	if !onUpdateEnable {
		return false, updateVal
	}

	if updateVal == "" || updateVal == emptyStrColAsignPart {
		defaultEnable, defaultVal := colSchemaValSetter(f, DBKeyDefault)
		if defaultEnable {
			updateVal = defaultVal
		}
	}

	return true, updateVal
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

func colSchemas(t reflect.Type) []*ColSchema {
	structFieldJSONMap := parseStructJSONMap(t)

	// 解析db相关标记
	var schemas []*ColSchema
	fields := reflectx.NewMapper(DBSchemaTag).TypeMap(t).Tree.Children
	for _, f := range fields {
		column := colSchema(f, structFieldJSONMap)
		if column == nil {
			continue
		}
		schemas = append(schemas, column)
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
		DBKeyUnique:        &column.Unique,
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

	column.setKeyAttrs()

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

func sqliteAutoIncrementColDeal(column *ColSchema) {
	column.AutoIncrement = false
	column.Primary = true
	column.Type = "INTEGER"
	column.setKeyAttrs()
	column.NotNull = false
}

func mysqlAutoIncrementColDeal(column *ColSchema) {
	column.Primary = true
	column.setKeyAttrs()
}
