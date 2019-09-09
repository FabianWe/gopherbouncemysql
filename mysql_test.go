// Copyright 2019 Fabian Wenzelmann
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gopherbouncemysql

import (
	"database/sql"
	"fmt"
	"github.com/FabianWe/gopherbouncedb"
	"github.com/FabianWe/gopherbouncedb/testsuite"
	"log"
	"os"
	"testing"
)

func setupPostgreConfigString() string {
	host := os.Getenv("MYSQL_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("MYSQL_PORT")
	if port == "" {
		port = "3306"
	}
	user := os.Getenv("MYSQL_USER")
	if user == "" {
		user = "mysql"
	}
	pw := os.Getenv("MYSQL_PASS")
	if pw == "" {
		pw = "password"
	}
	dbName := os.Getenv("MYSQL_DBNAME")
	if dbName == "" {
		dbName = "mysql"
	}
	config := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
		user, pw, host, port, dbName)
	return config
}

type mysqlTestBinding struct {
	db *sql.DB
}

func newMySQLTestBinding() *mysqlTestBinding {
	return &mysqlTestBinding{nil}
}

func removeData(db *sql.DB) error {
	stmt := `DROP TABLE IF EXISTS auth_user;`
	_, err := db.Exec(stmt)
	return err
}

func (b *mysqlTestBinding) BeginInstance() gopherbouncedb.UserStorage {
	// create db
	db, dbErr := sql.Open("mysql", setupPostgreConfigString())
	if dbErr != nil {
		panic(fmt.Sprintf("Can't create database: %s", dbErr.Error()))
	}
	// don't know exactly why this is required, but here we are
	db.SetMaxIdleConns(0)
	b.db = db
	// clear tables
	if removeErr := removeData(b.db); removeErr != nil {
		log.Printf("can't delete table entries: %s\n", removeErr.Error())
	}
	storage := NewMySQLUserStorage(db, nil)
	return storage
}

func (b *mysqlTestBinding) ClosteInstance(s gopherbouncedb.UserStorage) {
	if removeErr := removeData(b.db); removeErr != nil {
		log.Printf("can't delete table entries: %s\n", removeErr.Error())
	}
	if closeErr := b.db.Close(); closeErr != nil {
		panic(fmt.Sprintf("Can't close database: %s", closeErr.Error()))
	}
}

func TestInit(t *testing.T) {
	testsuite.TestInitSuite(newMySQLTestBinding(), t)
}

func TestInsert(t *testing.T) {
	testsuite.TestInsertSuite(newMySQLTestBinding(), true, t)
}

func TestLookup(t *testing.T) {
	testsuite.TestLookupSuite(newMySQLTestBinding(), true, t)
}

func TestUpdate(t *testing.T) {
	testsuite.TestUpdateUserSuite(newMySQLTestBinding(), true, t)
}

func TestDelete(t *testing.T) {
	testsuite.TestDeleteUserSuite(newMySQLTestBinding(), true, t)
}
