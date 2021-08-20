// Package sqlm 实现数据库对接的抽象封装,考虑性能和自由度要求不用ORM
package sqlm

import (
	"reflect"
	"sort"
	"sync"

	"github.com/jmoiron/sqlx"
)

var (
	// dbConCache store db connections for performance
	dbConCache = map[string]*sqlx.DB{}

	// database create drivers
	createDriversMu sync.RWMutex
	createDrivers   = map[string]DatabaseCreator{
		"mysql":   new(mysqlCreateImp),
		"sqlite3": new(sqlite3CreateImp),
	}
)

// TableAble is the interface that groups table curd methods.
type TableAble interface {
	Con() (*sqlx.DB, error)
	RowModel() interface{}
	SetRowModel(func() interface{})
	Create() error
	Insert(interface{}) (int64, error)
	Inserts([]interface{}) ([]int64, error)
	Save(interface{}) error
	Update(RowFilter, map[string]interface{}) error
	Delete(RowFilter) error
	Get(RowFilter, interface{}) error
	List(RowFilter, ListOptions) ([]interface{}, error)
	IsDup(interface{}) (interface{}, error)
}

type dbOptionSetter func(*sqlx.DB)

// RegisterDBCreator register database create for given driver.
func RegisterDBCreator(name string, driver DatabaseCreator) {
	createDriversMu.Lock()
	defer createDriversMu.Unlock()

	if driver == nil {
		panic("sqlm: RegisterDBCreator driver is nil")
	}

	if _, dup := createDrivers[name]; dup {
		panic("sqlm: RegisterDBCreator called twice for driver " + name)
	}

	createDrivers[name] = driver
}

// UnRegisterDBCreator uninstall database create driver.
func UnRegisterDBCreator(driver string) {
	createDriversMu.Lock()
	defer createDriversMu.Unlock()

	delete(createDrivers, driver)
}

// DBCreateDrivers returns a sorted list of the names of the registered create drivers.
func DBCreateDrivers() []string {
	createDriversMu.RLock()
	defer createDriversMu.RUnlock()

	var list []string
	for name := range createDrivers {
		list = append(list, name)
	}
	sort.Strings(list)

	return list
}

// DBCreateIterStructField 遍历配置模型的各个配置属性创建数据表.
func DBCreateIterStructField(val reflect.Value, optionSetter dbOptionSetter) error {
	var dbCons []*sqlx.DB

	for i := 0; i < val.NumField(); i++ {
		dbCon, err := createReflectTable(val.Field(i))
		if err != nil {
			return err
		}

		if dbCon != nil {
			dbCons = append(dbCons, dbCon)
		}
	}

	if optionSetter == nil {
		return nil
	}

	for _, c := range dbCons {
		optionSetter(c)
	}

	return nil
}

// createReflectTable 依据数据表反射值进行创建.
func createReflectTable(vf reflect.Value) (*sqlx.DB, error) {
	if vf.IsNil() || !vf.CanInterface() {
		return nil, nil
	}

	table, ok := vf.Interface().(TableAble)
	if !ok {
		return nil, nil
	}

	if err := table.Create(); err != nil {
		return nil, err
	}

	return table.Con()
}
