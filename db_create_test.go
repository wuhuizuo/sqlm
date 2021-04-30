package sqlm

import (
	"fmt"
	"os"
	"testing"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/auth"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/server"

	_ "github.com/go-sql-driver/mysql"
)

func Test_sqlite3CreateImp_Create(t *testing.T) {
	type args struct {
		dsn string
	}
	tests := []struct {
		name            string
		si              *sqlite3CreateImp
		args            args
		wantErr         bool
		fileWouldCreate string
	}{
		{
			"memory",
			new(sqlite3CreateImp),
			args{":memory:"},
			false,
			"",
		},
		{
			"file without params",
			new(sqlite3CreateImp),
			args{"file:hello.db"},
			false,
			"hello.db",
		},
		{
			"file without params",
			new(sqlite3CreateImp),
			args{"file:hello2.db?cache=shared&mode=memory"},
			false,
			"hello2.db",
		},
		{
			"file without file",
			new(sqlite3CreateImp),
			args{"file:"},
			true,
			"",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer os.Remove(tt.fileWouldCreate)

			si := &sqlite3CreateImp{}
			if err := si.Create(tt.args.dsn); (err != nil) != tt.wantErr {
				t.Errorf("sqlite3CreateImp.Create() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.fileWouldCreate != "" {
				if _, err := os.Stat(tt.fileWouldCreate); err != nil {
					t.Errorf("file not created: %s", tt.fileWouldCreate)
				}
			}
		})
	}
}

func Test_mysqlCreateImp_Create(t *testing.T) {
	fakeServer, err := newFakeMysqlServer()
	if err != nil {
		t.Fatal(err)
	}

	go func() { _ = fakeServer.Start() }()
	defer fakeServer.Close()

	type args struct {
		dsn string
	}
	tests := []struct {
		name    string
		mi      *mysqlCreateImp
		args    args
		wantErr bool
	}{
		{
			"invalid dsn-1",
			new(mysqlCreateImp),
			args{"xxx"},
			true,
		},
		{
			"invalid dsn missing slash",
			new(mysqlCreateImp),
			args{fmt.Sprintf("user:pass@tcp(%s)", fakeServer.Listener.Addr())},
			true,
		},
		{
			"valid dsn with existed database",
			new(mysqlCreateImp),
			args{fmt.Sprintf("user:pass@tcp(%s)/fake", fakeServer.Listener.Addr())},
			false,
		},
		{
			"valid dsn without db part",
			new(mysqlCreateImp),
			args{fmt.Sprintf("user:pass@tcp(%s)/", fakeServer.Listener.Addr())},
			true,
		},
		// The fake mysql server is not supported to create database statement.
		//	 https://github.com/dolthub/go-mysql-server/issues/250
		// {
		// 	"valid dsn with not existed database",
		// 	new(mysqlCreateImp),
		// 	args{fmt.Sprintf("user:pass@tcp(%s)/not_exist", fakeServer.Listener.Addr())},
		// 	false,
		// },
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mi := &mysqlCreateImp{}
			if err := mi.Create(tt.args.dsn); (err != nil) != tt.wantErr {
				t.Errorf("mysqlCreateImp.Create() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func newFakeMysqlServer() (*server.Server, error) {
	driver := sqle.NewDefault()
	db := memory.NewDatabase("fake")
	driver.AddDatabase(db)

	config := server.Config{
		Protocol: "tcp",
		Address:  "localhost:0",
		Auth:     auth.NewNativeSingle("user", "pass", auth.AllPermissions),
	}

	return server.NewDefaultServer(config, driver)
}
