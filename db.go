// Package sqlm 实现数据库对接的抽象封装,考虑性能和自由度要求不用ORM
package sqlm

import (
	"fmt"

	"github.com/jmoiron/sqlx"
)

// dbConCache store db connections for performance
var dbConCache = map[string]*sqlx.DB{}

// Database sql database
//	dns format:
//		mysql: 	 [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
//		sqlite3: file:test.db[?param1=value1&...&paramN=valueN]
type Database struct {
	Driver string `json:"driver"`
	DSN    string `json:"dsn"`
	dbCon  *sqlx.DB
}

// Con new/reuse db connection
func (p *Database) Con(create ...bool) *sqlx.DB {
	if p.dbCon == nil && p.Init(len(create) > 0 && create[0]) != nil {
		panic("db connect failed")
	}
	return p.dbCon
}

// Init db connection
func (p *Database) Init(create bool) error {
	// read con from cache
	conKey := fmt.Sprintf("%s://%s", p.Driver, p.DSN)
	if v, ok := dbConCache[conKey]; ok && v != nil {
		p.dbCon = v
		return nil
	}

	// 创建
	if create {
		if err := p.Create(); err != nil {
			return err
		}
	}

	db, err := sqlx.Open(p.Driver, p.DSN)
	if err != nil {
		return fmt.Errorf("db connect failed: %w", err)
	}

	dbConCache[conKey] = db
	p.dbCon = db

	return nil
}

// Close db connection
func (p *Database) Close() error {
	if p.dbCon == nil {
		return fmt.Errorf("db connection is not initialized")
	}
	err := p.dbCon.Close()
	p.dbCon = nil
	return err
}

// Create database schema if not exists.
func (p *Database) Create() error {
	switch p.Driver {
	case DriverMysql:
		return new(mysqlCreateImp).Create(p)
	case DriverSQLite3:
		return new(sqlite3CreateImp).Create(p)
	default:
		return fmt.Errorf("not support create db for driver %s", p.Driver)
	}
}
