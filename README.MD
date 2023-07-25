# Gibero (WIP)

 Tibero-Driver for Go's database/sql package


 ## install 

 `go get -u github.com/sankooc/gibero`

 ## usage

 
 ```golang
package main

import (
	"database/sql"
	_ "github.com/sankooc/gibero"
)

func main() {
	db, _ := sql.Open("tibero", "tibero://username:password@127.0.0.1:1521/dbname")
  // transaction
	tx, _ := db.Begin()
	db.Exec("insert into TEST_TABLE(NUM,VAR_B) values ( ?, ? )", .21, "SANKOOC-")
	tx.Commit()
  // query
  resultSet, _ := db.Query("SELECT ID, ACCOUNT FROM TEST_DB")
	for resultSet.Next() {
		var id int64
		var account string
    // support string,int64,float64,time.Time 
		resultSet.Scan(&id, &account)
		log.Printf("id %d %s \n", id, account)
	}
}

 ```