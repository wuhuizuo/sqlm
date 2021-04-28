package sqlm

import (
	"fmt"
	"os"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type DatabaseCreator interface {
	Create(*Database) error
}

type mysqlCreateImp struct{}

func (mi *mysqlCreateImp) Create(database *Database) error {
	if database == nil {
		return nil
	}

	db, err := sqlx.Open(database.Driver, database.DSN)
	if err != nil {
		return fmt.Errorf("host connect failed: %w", err)
	}
	defer db.Close()

	mysqlCfg, err := mysql.ParseDSN(database.DSN)
	if err != nil {
		return err
	}
	_, err = db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", mysqlCfg.DBName))
	return err
}

type sqlite3CreateImp struct{}

func (si *sqlite3CreateImp) Create(database *Database) error {
	if !strings.HasPrefix(database.DSN, "file:") {
		return nil
	}

	fileParts := strings.SplitN(database.DSN, "?", 2)
	if len(fileParts) == 0 {
		return fmt.Errorf("invalid sqlite3 dsn: %s", database.DSN)
	}

	file := strings.TrimPrefix(fileParts[0], "file:")

	return fileCreateIfNotExist(file)
}

func fileCreateIfNotExist(file string) error {
	_, err := os.Stat(file)
	if err == nil || !os.IsNotExist(err) {
		return err
	}

	_, err = os.Create(file)
	return err
}
