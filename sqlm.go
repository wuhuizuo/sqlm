// Package sqlm 实现数据库对接的抽象封装,考虑性能和自由度要求不用ORM
package sqlm

import (
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

	list := make([]string, 0, len(createDrivers))
	for name := range createDrivers {
		list = append(list, name)
	}
	sort.Strings(list)

	return list
}
