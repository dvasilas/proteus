package libbench

import (
	"database/sql"
	"fmt"

	"github.com/dvasilas/proteus/internal/libqpu"

	_ "github.com/go-sql-driver/mysql"
)

type datastore struct {
	db *sql.DB
}

func newDatastore(endpoint, datastoreDB, accessKeyID, secretAccessKey string) (datastore, error) {
	connStr := fmt.Sprintf("%s:%s@tcp(%s)/%s",
		accessKeyID,
		secretAccessKey,
		endpoint,
		datastoreDB,
	)

	db, err := sql.Open("mysql", connStr)
	if err != nil {
		return datastore{}, err
	}

	return datastore{db: db}, nil
}

// Insert ...
func (ds datastore) insert(table string, row map[string]interface{}) error {

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

	query := fmt.Sprintf("INSERT INTO %s %s VALUES %s", table, insertStmtAttrs, insertStmtAttrsValues)
	libqpu.Trace("insert", map[string]interface{}{"query": query, "insertValues": insertValues})
	stmtInsert, err := ds.db.Prepare(query)
	if err != nil {
		return err
	}

	_, err = stmtInsert.Exec(insertValues...)

	return err
}

func (ds datastore) get(table, projection string, predicate map[string]interface{}) (interface{}, error) {

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

	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s", projection, table, whereStmt)
	libqpu.Trace("get", map[string]interface{}{"query": query, "values": whereValues})
	stmtSelect, err := ds.db.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer stmtSelect.Close()

	var destValue interface{}
	err = stmtSelect.QueryRow(whereValues...).Scan(&destValue)

	return destValue, err
}
