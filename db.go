/*
Package db 实现数据库对接的抽象封装,考虑性能和自由度要求不用ORM
*/
package sqlm

import (
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"

	_ "github.com/go-sql-driver/mysql" // mysql driver
	_ "github.com/mattn/go-sqlite3"    // sqlite driver
)

// DatetimeFormat for store datetime column
const DatetimeFormat = "2006-01-02 15:04:05"

// dbConCache store db connections for performance
var dbConCache = map[string]*sqlx.DB{}

// Database sql database
type Database struct {
	Host          string `json:"host"`
	Port          int32  `json:"port"`
	DB            string `json:"db"`
	User          string `json:"user"`
	Password      string `json:"password"`
	Driver        string `json:"driver"`
	openImplement func(*Database, bool) (*sqlx.DB, error)
	dbCon         *sqlx.DB
}

func (p *Database) cacheKey() string {
	return fmt.Sprintf("%s://%s:%s@tcp(%s:%d)/%s", p.Driver, p.User, p.Password, p.Host, p.Port, p.DB)
}

// Init db connection
func (p *Database) Init(create bool) error {
	var db *sqlx.DB
	var err error

	// read con from cache
	conKey := p.cacheKey()
	if v, ok := dbConCache[conKey]; ok && v != nil {
		p.dbCon = v
		return nil
	}

	// 新开连接
	if p.openImplement == nil {
		switch p.Driver {
		case DriverMysql:
			db, err = p.OpenMysql(create)
		default:
			return fmt.Errorf("not implement driver: %s", p.Driver)
		}
	} else {
		// * 适配除mysql之外的数据库,支持自实现
		db, err = p.openImplement(p, create)
	}

	if err != nil {
		return fmt.Errorf("db connect failed: %w", err)
	}
	dbConCache[conKey] = db
	p.dbCon = db

	return nil
}

// OpenMysql for mysql db
func (p *Database) OpenMysql(create bool) (*sqlx.DB, error) {
	if create {
		err := p.create()
		if err != nil {
			return nil, fmt.Errorf("create db failed: %w", err)
		}
	}
	dataSource := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", p.User, p.Password, p.Host, p.Port, p.DB)
	return sqlx.Open(p.Driver, dataSource)
}

func (p *Database) create() error {
	dataSource := fmt.Sprintf("%s:%s@tcp(%s:%d)/", p.User, p.Password, p.Host, p.Port)
	db, err := sqlx.Open(p.Driver, dataSource)
	if err != nil {
		return fmt.Errorf("host connect failed: %w", err)
	}
	defer func() { _ = db.Close() }()

	_, err = db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", p.DB))
	return err
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

// Con db connection
func (p *Database) Con(create ...bool) *sqlx.DB {
	if p.dbCon == nil && p.Init(len(create) > 0 && create[0]) != nil {
		panic("db connect failed")
	}
	return p.dbCon
}

// SetOpenImplement for other db driver
func (p *Database) SetOpenImplement(imp func(*Database, bool) (*sqlx.DB, error)) {
	p.openImplement = imp
}

// ColTimeNow time now string for datetime column
func ColTimeNow() Datetime {
	return Datetime(time.Now().Format(DatetimeFormat))
}
