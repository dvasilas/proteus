package datastore

import (
	"database/sql"
	"fmt"
	"net"
	"time"

	//
	_ "github.com/go-sql-driver/mysql"
)

// Datastore ...
type Datastore struct {
	Db *sql.DB
}

// NewDatastore ...
func NewDatastore(endpoint, datastoreDB, accessKeyID, secretAccessKey string) (Datastore, error) {
	connStr := fmt.Sprintf("%s:%s@tcp(%s)/%s",
		accessKeyID,
		secretAccessKey,
		endpoint,
		datastoreDB,
	)

	for {
		c, err := net.DialTimeout("tcp", endpoint, time.Second)
		if err != nil {
			time.Sleep(1 * time.Second)
			fmt.Println("retying connecting to ", endpoint)
		} else {
			c.Close()
			break
		}
	}

	db, err := sql.Open("mysql", connStr)
	if err != nil {
		return Datastore{}, err
	}

	return Datastore{Db: db}, nil
}

// Insert ...
func (ds Datastore) Insert(table string, row map[string]interface{}) error {

	insertStmtAttrs := "("
	insertStmtAttrsValues := "("
	insertValues := make([]interface{}, len(row))

	i := 0
	for k, v := range row {
		insertStmtAttrs += k
		insertStmtAttrsValues += "?"
		if i < len(row)-1 {
			insertStmtAttrs += ", "
			insertStmtAttrsValues += ", "
		}
		insertValues[i] = v
		i++
	}

	insertStmtAttrs += ")"
	insertStmtAttrsValues += ")"

	query := "INSERT INTO " + table + " " + insertStmtAttrs + " VALUES " + insertStmtAttrsValues
	stmtInsert, err := ds.Db.Prepare(query)
	if err != nil {
		return err
	}
	defer stmtInsert.Close()

	_, err = stmtInsert.Exec(insertValues...)

	return err
}

// Get ...
func (ds Datastore) Get(table, projection string, predicate map[string]interface{}) (interface{}, error) {

	whereStmt := ""
	whereValues := make([]interface{}, len(predicate))
	i := 0

	for attrKey, val := range predicate {
		whereStmt += fmt.Sprintf("%s = ? ", attrKey)
		if len(predicate) > 1 && i < len(predicate)-1 {
			whereStmt += "AND "
		}
		whereValues[i] = val
		i++
	}

	query := "SELECT " + projection + " FROM " + table + " WHERE " + whereStmt
	stmtSelect, err := ds.Db.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer stmtSelect.Close()

	var destValue interface{}
	err = stmtSelect.QueryRow(whereValues...).Scan(&destValue)

	return destValue, err
}
