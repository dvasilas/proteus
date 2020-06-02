package mysqlbackend

import (
	"database/sql"
	"fmt"

	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/proto/qpu"
	ptypes "github.com/golang/protobuf/ptypes"
	timestamp "github.com/golang/protobuf/ptypes/timestamp"
)

// This package provides an implementation of the libqpu.QPUState interface
// (internal/libqpu/types.go) for a MySQL database.
// It essenitally provides wrappers for for storing and retrieving the QPU's
// state in a MySQL database.

// MySQLStateBackend implements the libqpu.QPUState interface using MySQL as a
// backend store.
type MySQLStateBackend struct {
	db *sql.DB
}

// NewStateBackend initiated a connection with the MySQL specified in the QPU's
// configuration, and creates an instance of libqpu.QPUState.
func NewStateBackend(conf *libqpu.QPUConfig) (MySQLStateBackend, error) {
	connStr := fmt.Sprintf("%s:%s@tcp(%s)/",
		conf.StateBackend.Credentials.AccessKeyID,
		conf.StateBackend.Credentials.SecretAccessKey,
		conf.StateBackend.Endpoint,
	)
	db, err := sql.Open("mysql", connStr)
	if err != nil {
		return MySQLStateBackend{}, err
	}
	return MySQLStateBackend{db: db}, nil
}

// Init performs the necessary initializations for using the MySQL instance as
// a store for the QPU's state.
// In more detail:
// - it creates a database if it does not exist, selects it (USE)
// - and creates the table to be used for storing the state (it drops the table
//   if it already exists)
func (s MySQLStateBackend) Init(database, table, createTable string) error {
	if _, err := s.db.Exec("CREATE DATABASE IF NOT EXISTS " + database); err != nil {
		return err
	}

	if _, err := s.db.Exec("USE " + database); err != nil {
		return err
	}

	if _, err := s.db.Exec("DROP TABLE IF EXISTS " + table); err != nil {
		return err
	}

	libqpu.Trace("creating table", map[string]interface{}{"stmt": createTable})
	if _, err := s.db.Exec(createTable); err != nil {
		return err
	}

	return nil
}

// Insert inserts a record in the state.
func (s MySQLStateBackend) Insert(table, columns, values string, args ...interface{}) error {
	query := fmt.Sprintf("INSERT INTO %s %s VALUES %s", table, columns, values)
	libqpu.Trace("insert", map[string]interface{}{"query": query, "args": args})
	stmtInsert, err := s.db.Prepare(query)
	if err != nil {
		return err
	}

	_, err = stmtInsert.Exec(args...)

	return err
}

// Update updates a state record.
func (s MySQLStateBackend) Update(table, update, condition string, args ...interface{}) error {
	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s", table, update, condition)
	libqpu.Trace("update", map[string]interface{}{"query": query, "args": args})
	stmtInsert, err := s.db.Prepare(query)
	if err != nil {
		return err
	}

	_, err = stmtInsert.Exec(args...)

	return err
}

// Get retrieves a state record, and returns the values specified by 'projection'
func (s MySQLStateBackend) Get(projection, table, condition string, args ...interface{}) (interface{}, error) {
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s", projection, table, condition)
	libqpu.Trace("get", map[string]interface{}{"query": query, "args": args})
	stmtSelect, err := s.db.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer stmtSelect.Close()

	var destValue interface{}
	err = stmtSelect.QueryRow(args...).Scan(&destValue)

	return destValue, err
}

// Scan retrieves all state records.
// It returns a channel that can be used to iteratively return the retrieved records.
// The channel returns records of type map[<attributeName>]<string_value>.
func (s MySQLStateBackend) Scan(table string, columns []string) (<-chan map[string]string, error) {
	projection := ""

	columns = append(columns, "ts_key")
	columns = append(columns, "unix_timestamp(ts)")

	for i, col := range columns {
		projection += col
		if i < len(columns)-1 {
			projection += ", "
		}
	}

	query := fmt.Sprintf("SELECT %s FROM %s", projection, table)
	rows, err := s.db.Query(query)
	if err != nil {
		return nil, err
	}
	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	resultCh := make(chan map[string]string)
	go func() {
		for rows.Next() {
			err = rows.Scan(scanArgs...)
			if err != nil {
				panic(err)
			}
			row := make(map[string]string, 0)
			for i, col := range values {
				if col != nil {
					row[columns[i]] = string(col)
				}
			}
			resultCh <- row
		}
		close(resultCh)
	}()
	return resultCh, nil
}

// Cleanup closes the connection to the MySQL instance
func (s MySQLStateBackend) Cleanup() {
	s.db.Close()
}

// ConstructSelect ...
func ConstructSelect(predicate map[string]*qpu.Value) (string, []interface{}) {
	whereStmt := ""
	whereValues := make([]interface{}, len(predicate))
	i := 0

	for attrKey, val := range predicate {
		whereStmt += fmt.Sprintf("%s = ? ", attrKey)
		if len(predicate) > 1 && i < len(predicate)-1 {
			whereStmt += "AND "
		}
		whereValues[i] = val.GetInt()
		i++
	}

	return whereStmt, whereValues
}

// ConstructInsert ...
func ConstructInsert(attrToSum string, valToInsert int64, predicate map[string]*qpu.Value, vc map[string]*timestamp.Timestamp) (string, string, []interface{}) {
	attrKeyStmt := fmt.Sprintf("(%s", attrToSum)
	attrValStmt := "(?"
	insertValues := make([]interface{}, len(predicate)+1+2)
	i := 0
	insertValues[i] = valToInsert
	i++

	for attrKey, val := range predicate {
		attrKeyStmt += ", " + attrKey
		attrValStmt += ", ?"

		insertValues[i] = val.GetInt()
		i++
	}

	for k, v := range vc {
		insertValues[i] = k
		i++
		ts, err := ptypes.Timestamp(v)
		if err != nil {
			panic(err)
		}
		insertValues[i] = ts
		i++
	}

	attrKeyStmt += ", ts_key, ts)"
	attrValStmt += ", ?, ?)"

	return attrKeyStmt, attrValStmt, insertValues
}

// ConstructUpdate ...
func ConstructUpdate(attrToSum string, valToInsert int64, predicate map[string]*qpu.Value, vc map[string]*timestamp.Timestamp) (string, []interface{}) {
	updateValues := make([]interface{}, len(predicate)+1+2)
	i := 0
	updateValues[i] = valToInsert
	i++

	for k, v := range vc {
		updateValues[i] = k
		i++
		ts, err := ptypes.Timestamp(v)
		if err != nil {
			panic(err)
		}
		updateValues[i] = ts
		i++
	}

	for _, val := range predicate {
		updateValues[i] = val.GetInt()
		i++
	}

	return fmt.Sprintf("%s = ?, ts_key = ?, ts = ?", attrToSum), updateValues
}
