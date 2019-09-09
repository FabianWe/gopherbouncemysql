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
	"errors"
	"fmt"
	"github.com/FabianWe/gopherbouncedb"
	"github.com/go-sql-driver/mysql"
	"reflect"
	"strings"
	"time"
)

var (
	DefaultMySQLUserRowNames = gopherbouncedb.DefaultUserRowNames
)

const (
	// MySQLKeyExists is the error key in the mysql driver that states
	// that a key error occurred.
	MySQLKeyExists = 1062
)

// MySQLQueries implements gopherbouncedb.UserSQL with support for MySQL.
type MySQLQueries struct {
	InitS []string
	GetUserS, GetUserByNameS, GetUserByEmailS, InsertUserS,
	UpdateUserS, DeleteUserS, UpdateFieldsS string
	Replacer *gopherbouncedb.SQLTemplateReplacer
	RowNames map[string]string
}

func DefaultMySQLReplacer() *gopherbouncedb.SQLTemplateReplacer {
	return gopherbouncedb.DefaultSQLReplacer()
}

func NewMySQLQueries(replaceMapping map[string]string) *MySQLQueries {
	replacer := DefaultMySQLReplacer()
	if replaceMapping != nil {
		replacer.UpdateDict(replaceMapping)
	}
	res := &MySQLQueries{}
	res.Replacer = replacer
	// first all init strings
	res.InitS = append(res.InitS, replacer.Apply(MySQLUsersInit))
	res.GetUserS = replacer.Apply(MySQLQueryUserID)
	res.GetUserByNameS = replacer.Apply(MySQLQueryUsername)
	res.GetUserByEmailS = replacer.Apply(MySQLQueryUserEmail)
	res.InsertUserS = replacer.Apply(MySQLInsertUser)
	res.UpdateUserS = replacer.Apply(MySQLUpdateUser)
	res.DeleteUserS = replacer.Apply(MySQLDeleteUser)
	res.UpdateFieldsS = replacer.Apply(MySQLUpdateUserFields)
	res.RowNames = DefaultMySQLUserRowNames
	return res
}

func (q *MySQLQueries) InitUsers() []string {
	return q.InitS
}

func (q *MySQLQueries) GetUser() string {
	return q.GetUserS
}

func (q *MySQLQueries) GetUserByName() string {
	return q.GetUserByNameS
}

func (q *MySQLQueries) GetUserByEmail() string {
	return q.GetUserByEmailS
}

func (q *MySQLQueries) InsertUser() string {
	return q.InsertUserS
}

func (q *MySQLQueries) UpdateUser(fields []string) string {
	if len(fields) == 0 || !q.SupportsUserFields() {
		return q.UpdateUserS
	}
	updates := make([]string, len(fields))
	for i, fieldName := range fields {
		if colName, has := q.RowNames[fieldName]; has {
			updates[i] = colName + "=?"
		} else {
			panic(fmt.Sprintf("invalid field name \"%s\": Must be a valid field name of gopherbouncedb.UserModel", fieldName))
		}
	}
	updateStr := strings.Join(updates, ",")
	stmt := strings.Replace(q.UpdateFieldsS, "$UPDATE_CONTENT$", updateStr, 1)
	return stmt
}

func (q *MySQLQueries) DeleteUser() string {
	return q.DeleteUserS
}

func (q *MySQLQueries) SupportsUserFields() bool {
	return q.UpdateFieldsS != ""
}

type MySQLBridge struct{}

func NewMySQLBridge() MySQLBridge {
	return MySQLBridge{}
}

func (b MySQLBridge) TimeScanType() interface{} {
	var res mysql.NullTime
	return &res
}

func (b MySQLBridge) ConvertTimeScanType(val interface{}) (time.Time, error) {
	var zeroT time.Time
	var nt mysql.NullTime
	switch v := val.(type) {
	case *mysql.NullTime:
		nt = *v
	case mysql.NullTime:
		nt = v
	default:
		return zeroT, fmt.Errorf("MySQLBridge.ConvertScanType: Expected value of *mysql.NullTime, got %v",
			reflect.TypeOf(val))
	}
	if !nt.Valid {
		return zeroT, errors.New("MySQLBridge.ConvertScanType: got NULL datetime, expected to be not NULL")
	}
	return nt.Time, nil
}

func (b MySQLBridge) ConvertTime(t time.Time) interface{} {
	return t
}

func (b MySQLBridge) IsDuplicateInsert(err error) bool {
	if mysqlErr, ok := err.(*mysql.MySQLError); ok && mysqlErr.Number == MySQLKeyExists {
		return true
	}
	return false
}

func (b MySQLBridge) IsDuplicateUpdate(err error) bool {
	if mysqlErr, ok := err.(*mysql.MySQLError); ok && mysqlErr.Number == MySQLKeyExists {
		return true
	}
	return false
}

type MySQLUserStorage struct {
	*gopherbouncedb.SQLUserStorage
}

func NewMySQLUserStorage(db *sql.DB, replaceMapping map[string]string) *MySQLUserStorage {
	queries := NewMySQLQueries(replaceMapping)
	bridge := NewMySQLBridge()
	sqlStorage := gopherbouncedb.NewSQLUserStorage(db, queries, bridge)
	res := MySQLUserStorage{sqlStorage}
	return &res
}
