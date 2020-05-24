package mysqlbackend

import (
	"database/sql"
	"fmt"

	"github.com/dvasilas/proteus/internal/libqpu"
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

	if _, err := s.db.Exec(createTable); err != nil {
		return err
	}

	return nil
}

// Insert inserts a record in the state.
func (s MySQLStateBackend) Insert(table, columns, values string, args ...interface{}) error {
	query := fmt.Sprintf("INSERT INTO %s %s VALUES %s", table, columns, values)
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
