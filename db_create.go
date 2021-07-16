package sqlm

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/jmoiron/sqlx"
)

type DatabaseCreator interface {
	Create(dsn string) error
}

type mysqlCreateImp struct{}

func (*mysqlCreateImp) Create(dsn string) error {
	db, err := sqlx.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("host connect failed: %w", err)
	}
	defer db.Close()

	dbName := getDBFromMysqlDSN(dsn)
	if dbName == "" {
		return errors.New("not found db name from dsn")
	}

	_, err = db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName))
	return err
}

type sqlite3CreateImp struct{}

func (*sqlite3CreateImp) Create(dsn string) error {
	if !strings.HasPrefix(dsn, "file:") {
		return nil
	}

	fileParts := strings.SplitN(dsn, "?", 2)
	file := strings.TrimPrefix(fileParts[0], "file:")

	return fileCreateIfNotExist(file)
}

func fileCreateIfNotExist(file string) error {
	if file == "" {
		return errors.New("empty file name")
	}

	_, err := os.Stat(file)
	if err != nil && os.IsNotExist(err) {
		_, err = os.Create(file)
	}

	return err
}

func getDBFromMysqlDSN(dsn string) string {
	parts := strings.Split(dsn, "/")

	// nolint: gomnd
	if len(parts) < 2 {
		return ""
	}

	dbPart := parts[len(parts)-1]

	return strings.SplitN(dbPart, "?", 2)[0]
}

func getMysqlDSNForCreate(dsn string) string {
	parts := strings.SplitN(dsn, "@", 2)
	if len(parts) < 2 {
		return dsn
	}

	hostAndPathPart := parts[1]
	parts = strings.SplitN(hostAndPathPart, "/", 2)
	// nolint: gomnd
	if len(parts) < 2 {
		return dsn
	}

	return strings.TrimSuffix(dsn, parts[len(parts)-1])
}
