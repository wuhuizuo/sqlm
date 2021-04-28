package sqlm

import (
	"fmt"
	"os"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type DatabaseCreator interface {
	Create(dsn string) error
}

type mysqlCreateImp struct{}

func (mi *mysqlCreateImp) Create(dsn string) error {
	db, err := sqlx.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("host connect failed: %w", err)
	}
	defer db.Close()

	mysqlCfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return err
	}
	_, err = db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", mysqlCfg.DBName))
	return err
}

type sqlite3CreateImp struct{}

func (si *sqlite3CreateImp) Create(dsn string) error {
	if !strings.HasPrefix(dsn, "file:") {
		return nil
	}

	fileParts := strings.SplitN(dsn, "?", 2)
	if len(fileParts) == 0 {
		return fmt.Errorf("invalid sqlite3 dsn: %s", dsn)
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
