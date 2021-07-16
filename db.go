package sqlm

import (
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"
)

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
func (p *Database) Con() (*sqlx.DB, error) {
	if p.dbCon != nil {
		return p.dbCon, nil
	}

	// read con from cache.
	conKey := fmt.Sprintf("%s://%s", p.Driver, p.DSN)
	if db, ok := dbConCache[conKey]; ok && db != nil {
		p.dbCon = db
		return db, nil
	}

	db, err := p.newCon()
	if err == nil {
		dbConCache[conKey] = db
		p.dbCon = db
	}

	return db, err
}

func (p *Database) newCon() (*sqlx.DB, error) {
	db, err := sqlx.Open(p.Driver, p.DSN)
	if err != nil {
		return nil, fmt.Errorf("db connect failed: %w", err)
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
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

// SetCreateor set database creator.
func (p *Database) Create() error {
	if p.Driver == "" {
		return errors.New("driver is not setted")
	}

	creator := createDrivers[p.Driver]
	if creator == nil {
		return fmt.Errorf("non create driver registered for driver %s", p.Driver)
	}

	return creator.Create(p.DSN)
}
