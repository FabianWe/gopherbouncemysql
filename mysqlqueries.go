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

const (
	MySQLUsersInit = `CREATE TABLE IF NOT EXISTS $USERS_TABLE_NAME$ (
id BIGINT AUTO_INCREMENT,
username VARCHAR(150) NOT NULL UNIQUE,
password VARCHAR(270) NOT NULL,
email VARCHAR(254) NOT NULL $EMAIL_UNIQUE$,
first_name VARCHAR(50) NOT NULL,
last_name VARCHAR(150) NOT NULL,
is_superuser BOOL NOT NULL,
is_staff BOOL NOT NULL,
is_active BOOL NOT NULL,
date_joined DATETIME NOT NULL,
last_login DATETIME NOT NULL,
PRIMARY KEY(id)
);
`
	MySQLQueryUserID = `SELECT * FROM $USERS_TABLE_NAME$ WHERE id=?;`

	MySQLQueryUsername = `SELECT * FROM $USERS_TABLE_NAME$ WHERE username=?;`

	MySQLQueryUserEmail = `SELECT * FROM $USERS_TABLE_NAME$ WHERE email=?;`

	MySQLInsertUser = `INSERT INTO $USERS_TABLE_NAME$(
username, password, email, first_name, last_name, is_superuser, is_staff,
is_active, date_joined, last_login)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`

	MySQLUpdateUser = `UPDATE $USERS_TABLE_NAME$
SET username=?, password=?, email=?, first_name=?, last_name=?,
	is_superuser=?, is_staff=?, is_active=?, date_joined=?, last_login=?
WHERE id=?;`

	MySQLDeleteUser = `DELETE FROM $USERS_TABLE_NAME$ WHERE id=?;`

	MySQLUpdateUserFields = `UPDATE $USERS_TABLE_NAME$
SET $UPDATE_CONTENT$
WHERE id=?;`
)
