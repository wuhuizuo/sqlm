package sqlm

import (
	"fmt"
	"testing"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3" // sqlite3 driver
)

func TestDatabase_Create(t *testing.T) {
	fakeServer, err := newFakeMysqlServer()
	if err != nil {
		t.Fatal(err)
	}

	go func() { _ = fakeServer.Start() }()
	defer fakeServer.Close()

	type fields struct {
		Driver string
		DSN    string
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			"empty dsn",
			fields{"mysql", ""},
			true,
		},
		{
			"empty driver",
			fields{"", ":memory:"},
			true,
		},
		{
			"sqlite3 memory",
			fields{"sqlite3", ":memory:"},
			false,
		},
		{
			"valid mysql",
			fields{"mysql", fmt.Sprintf("user:pass@tcp(%s)/fake", fakeServer.Listener.Addr())},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Database{
				Driver: tt.fields.Driver,
				DSN:    tt.fields.DSN,
			}
			if err := p.Create(); (err != nil) != tt.wantErr {
				t.Errorf("Database.Create() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}

	t.Run("empty create drivers", func(t *testing.T) {
		UnRegisterDBCreator("mysql")
		defer RegisterDBCreator("mysql", new(mysqlCreateImp))

		p := &Database{
			Driver: "mysql",
			DSN:    fmt.Sprintf("user:pass@tcp(%s)/fake", fakeServer.Listener.Addr()),
		}

		if p.Create() == nil {
			t.Errorf("Database.Create() no error, but want error occuried")
		}
	})
}

func TestDatabase_Close(t *testing.T) {
	type fields struct {
		Driver string
		DSN    string
		open   bool
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			"no con",
			fields{"sqlite3", ":memory:", false},
			true,
		},
		{
			"has con",
			fields{"sqlite3", ":memory:", true},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Database{
				Driver: tt.fields.Driver,
				DSN:    tt.fields.DSN,
			}
			if tt.fields.open {
				if err := p.Init(true); err != nil {
					t.Fatal(err)
				}
			}

			if err := p.Close(); (err != nil) != tt.wantErr {
				t.Errorf("Database.Close() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDatabase_Init(t *testing.T) {
	fakeServer, err := newFakeMysqlServer()
	if err != nil {
		t.Fatal(err)
	}

	go func() { _ = fakeServer.Start() }()
	defer fakeServer.Close()

	type fields struct {
		Driver string
		DSN    string
	}
	type args struct {
		create bool
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"sqlite memory",
			fields{"sqlite3", ":memory:"},
			args{false},
			false,
		},
		{
			"mysql database create ok",
			fields{"mysql", fmt.Sprintf("user:pass@tcp(%s)/not_exist", fakeServer.Listener.Addr())},
			args{true},
			false,
		},
		{
			"new connection fail at sql open stage",
			fields{"unknown", fmt.Sprintf("user:pass@tcp(%s)/fake", fakeServer.Listener.Addr())},
			args{false},
			true,
		},
		{
			"connect to a not existed database failed",
			fields{"mysql", fmt.Sprintf("user:pass@tcp(%s)/not_exist2", fakeServer.Listener.Addr())},
			args{false},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Database{
				Driver: tt.fields.Driver,
				DSN:    tt.fields.DSN,
			}
			if err := p.Init(tt.args.create); (err != nil) != tt.wantErr {
				t.Errorf("Database.Init() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDatabase_Con(t *testing.T) {
	fakeServer, err := newFakeMysqlServer()
	if err != nil {
		t.Fatal(err)
	}

	go func() { _ = fakeServer.Start() }()
	defer fakeServer.Close()

	type fields struct {
		Driver string
		DSN    string
		dbCon  *sqlx.DB
	}
	type args struct {
		create []bool
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"has conn, no args",
			fields{"sqlite3", ":memory:", sqlx.MustOpen("sqlite3", ":memory:")},
			args{[]bool{}},
			false,
		},
		{
			"not connected, no args",
			fields{"sqlite3", ":memory:", nil},
			args{[]bool{}},
			false,
		},
		{
			"not connected, no args, init falled",
			fields{"mysql", fmt.Sprintf("user:pass@tcp(%s)/not_exist", fakeServer.Listener.Addr()), nil},
			args{[]bool{}},
			true,
		},
		{
			"not connected, need create, init ok",
			fields{"mysql", fmt.Sprintf("user:pass@tcp(%s)/fake", fakeServer.Listener.Addr()), nil},
			args{[]bool{true}},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Database{
				Driver: tt.fields.Driver,
				DSN:    tt.fields.DSN,
				dbCon:  tt.fields.dbCon,
			}
			if _, err := p.Con(tt.args.create...); (err != nil) != tt.wantErr {
				t.Errorf("Database.Con() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
