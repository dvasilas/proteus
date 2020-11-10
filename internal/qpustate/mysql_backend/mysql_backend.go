package mysqlbackend

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/libqpu/utils"
	"github.com/dvasilas/proteus/pkg/proteus-go-client/pb"
	ptypes "github.com/golang/protobuf/ptypes"
	timestamp "github.com/golang/protobuf/ptypes/timestamp"
	"github.com/opentracing/opentracing-go"

	//
	_ "github.com/go-sql-driver/mysql"
)

// This package provides an implementation of the libqpu.QPUState interface
// (internal/libqpu/types.go) for a MySQL database.
// It essenitally provides wrappers for for storing and retrieving the QPU's
// state in a MySQL database.

// MySQLStateBackend implements the libqpu.QPUState interface using MySQL as a
// backend store.
type MySQLStateBackend struct {
	db              *sql.DB
	accessKeyID     string
	secretAccessKey string
	endpoint        string
	logTimestamps   bool
}

// NewStateBackend initiated a connection with the MySQL specified in the QPU's
// configuration, and creates an instance of libqpu.QPUState.
func NewStateBackend(conf *libqpu.QPUConfig) (*MySQLStateBackend, error) {
	// connStr := fmt.Sprintf("%s:%s@tcp(%s)/",
	// 	conf.StateBackend.Credentials.AccessKeyID,
	// 	conf.StateBackend.Credentials.SecretAccessKey,
	// 	conf.StateBackend.Endpoint,
	// )
	// db, err := sql.Open("mysql", connStr)
	// if err != nil {
	// 	return MySQLStateBackend{}, err
	// }
	// return MySQLStateBackend{db: db}, nil
	return &MySQLStateBackend{
		accessKeyID:     conf.StateBackend.Credentials.AccessKeyID,
		secretAccessKey: conf.StateBackend.Credentials.SecretAccessKey,
		endpoint:        conf.StateBackend.Endpoint,
		logTimestamps:   conf.Evaluation.LogTimestamps,
	}, nil
}

// Init performs the necessary initializations for using the MySQL instance as
// a store for the QPU's state.
// In more detail:
// - it creates a database if it does not exist, selects it (USE)
// - and creates the table to be used for storing the state (it drops the table
//   if it already exists)
func (s *MySQLStateBackend) Init(database, table, createTable string) error {
	connStr := fmt.Sprintf("%s:%s@tcp(%s)/",
		s.accessKeyID,
		s.secretAccessKey,
		s.endpoint,
	)
	db, err := sql.Open("mysql", connStr)
	if err != nil {
		return utils.Error(err)
	}

	if _, err = db.Exec("CREATE DATABASE IF NOT EXISTS " + database); err != nil {
		return utils.Error(err)
	}

	if _, err = db.Exec("USE " + database); err != nil {
		return utils.Error(err)
	}

	if _, err = db.Exec("DROP TABLE IF EXISTS " + table); err != nil {
		return utils.Error(err)
	}

	// utils.Trace("creating table", map[string]interface{}{"stmt": createTable})
	if _, err = db.Exec(createTable); err != nil {
		return utils.Error(err)
	}

	if s.logTimestamps {
		if _, err = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s_write_log", table)); err != nil {
			return utils.Error(err)
		}
		createTSTable := fmt.Sprintf(
			"CREATE TABLE %s_write_log (row_id INT, ts DATETIME(6), ts_local DATETIME(6))",
			table,
		)
		if _, err = db.Exec(createTSTable); err != nil {
			return utils.Error(err)
		}
		if _, err = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s_query_log", table)); err != nil {
			return utils.Error(err)
		}
		createTSTable = fmt.Sprintf(
			"CREATE TABLE %s_query_log (row_ids VARCHAR(30), ts_local DATETIME(6))",
			table,
		)
		if _, err = db.Exec(createTSTable); err != nil {
			return utils.Error(err)
		}
	}

	db.Close()

	connStr = fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true&interpolateParams=true",
		s.accessKeyID,
		s.secretAccessKey,
		s.endpoint,
		database,
	)
	db, err = sql.Open("mysql", connStr)
	if err != nil {
		return utils.Error(err)
	}

	// echo 1 > /proc/sys/net/ipv4/tcp_tw_reuse
	db.SetMaxIdleConns(2048)
	db.SetMaxOpenConns(2048)
	db.SetConnMaxLifetime(10 * time.Minute)

	s.db = db

	return nil
}

// Insert inserts a record in the state.
func (s *MySQLStateBackend) Insert(table string, row map[string]interface{}, vc map[string]*timestamp.Timestamp, rowIDVal interface{}) error {

	var ts time.Time
	var err error

	insertStmtAttrs := "("
	insertStmtAttrsValues := "("
	insertValues := make([]interface{}, len(row)+2)

	i := 0
	for k, v := range row {
		insertStmtAttrs += k + ", "
		insertStmtAttrsValues += "?, "
		// insertValues[i] = v.GetInt()
		insertValues[i] = v
		i++
	}

	for k, v := range vc {
		insertValues[i] = k
		i++
		ts, err = ptypes.Timestamp(v)
		if err != nil {
			panic(err)
		}
		insertValues[i] = ts
		i++
	}

	insertStmtAttrs += "ts_key, ts)"
	insertStmtAttrsValues += "?, ?)"

	query := fmt.Sprintf("INSERT INTO %s %s VALUES %s", table, insertStmtAttrs, insertStmtAttrsValues)
	// utils.Trace("insert", map[string]interface{}{"query": query, "insertValues": insertValues})

	stmtInsert, err := s.db.Prepare(query)
	if err != nil {
		return err
	}

	_, err = stmtInsert.Exec(insertValues...)
	if err != nil {
		return err
	}

	stmtInsert.Close()

	if s.logTimestamps {
		go func() {
			tsLocal := time.Now()
			query = fmt.Sprintf("INSERT INTO %s_write_log (row_id, ts, ts_local) VALUES (?,?,?)", table)
			stmtInsert, err = s.db.Prepare(query)
			if err != nil {
				log.Fatal(err)
			}
			defer stmtInsert.Close()
			_, err = stmtInsert.Exec(rowIDVal, ts, tsLocal)
		}()
	}

	return err
}

// Update updates a state record.
func (s *MySQLStateBackend) Update(table string, predicate, newValues map[string]interface{}, vc map[string]*timestamp.Timestamp, rowIDVal interface{}) error {

	var ts time.Time
	var err error

	updateStmt := ""
	whereStmt := ""
	updateValues := make([]interface{}, len(newValues)+len(predicate)+2)
	i := 0

	for k, v := range newValues {
		updateStmt += k + " = ?, "
		updateValues[i] = v

		i++
	}

	updateStmt += "ts_key = ?, ts = ?"

	for k, v := range vc {
		updateValues[i] = k
		i++
		ts, err = ptypes.Timestamp(v)
		if err != nil {
			panic(err)
		}
		updateValues[i] = ts
		i++
	}

	j := 0
	for k, v := range predicate {
		whereStmt += fmt.Sprintf("%s = ? ", k)
		if len(predicate) > 1 && j < len(predicate)-1 {
			whereStmt += "AND "
		}
		updateValues[i] = v
		i++
		j++
	}

	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s", table, updateStmt, whereStmt)
	// utils.Trace("update", map[string]interface{}{"query": query, "updateValues": updateValues})

	stmtUpdate, err := s.db.Prepare(query)
	if err != nil {
		return err
	}

	_, err = stmtUpdate.Exec(updateValues...)
	if err != nil {
		return err
	}

	stmtUpdate.Close()

	if s.logTimestamps {
		go func() {
			var stmtInsert *sql.Stmt
			tsLocal := time.Now()
			query = fmt.Sprintf("INSERT INTO %s_write_log (row_id, ts, ts_local) VALUES (?,?,?)", table)
			// utils.Trace("update", map[string]interface{}{"query": query})
			stmtInsert, err = s.db.Prepare(query)
			if err != nil {
				log.Fatal(err)
			}
			defer stmtInsert.Close()
			_, err = stmtInsert.Exec(rowIDVal, ts, tsLocal)
		}()
	}

	return err
}

// GetRow ...
func (s *MySQLStateBackend) GetRow(from string, projection []string, where []string, parentSpan opentracing.Span) *sql.Row {
	projectionStmt := ""
	for i, col := range projection {
		projectionStmt += col
		if i < len(projection)-1 {
			projectionStmt += ", "
		}
	}

	// prepare predicate
	whereStmt := ""
	if where != nil && len(where) > 0 {
		whereStmt += "WHERE "
		i := 0

		for _, wherePred := range where {
			whereStmt += wherePred
			if len(where) > 1 && i < len(where)-1 {
				whereStmt += "AND "
			}
			i++
		}
	}

	query := fmt.Sprintf("SELECT %s FROM %s %s", projectionStmt, from, whereStmt)
	// utils.Trace("QueryRow", map[string]interface{}{"query": query})

	return s.db.QueryRow(query)
}

// Get retrieves state records based on a given query.
// It returns a channel that can be used to iteratively return the retrieved records.
// The channel returns records of type map[<attributeName>]<string_value>.
func (s *MySQLStateBackend) Get(from string, projection []string, where []string, orderby string, limit int64, parentSpan opentracing.Span) (<-chan map[string]interface{}, error) {
	// tracing
	// var tracer opentracing.Tracer
	// var dbQuerySp opentracing.Span
	// dbQuerySp = nil
	// tracer = nil
	// if parentSpan != nil {
	// 	tracer = opentracing.GlobalTracer()
	// }

	// prepare projection
	projectionStmt := ""
	for i, col := range projection {
		projectionStmt += col
		if i < len(projection)-1 {
			projectionStmt += ", "
		}
	}

	// prepare predicate
	whereStmt := ""
	if where != nil && len(where) > 0 {
		whereStmt += "WHERE "
		i := 0

		for _, wherePred := range where {
			whereStmt += wherePred
			if len(where) > 1 && i < len(where)-1 {
				whereStmt += "AND "
			}
			i++
		}
	}

	// prepare order-by
	orderbyStmt := ""
	if len(orderby) > 0 {
		whereStmt = "ORDER BY " + orderby
	}

	// prepare limit
	limitStmt := ""
	if limit > 0 {
		limitStmt = "LIMIT " + strconv.Itoa(int(limit))
	}

	query := fmt.Sprintf("SELECT %s FROM %s %s %s %s", projectionStmt, from, whereStmt, orderbyStmt, limitStmt)
	// utils.Trace("query", map[string]interface{}{"query": query})

	// tracing
	// if tracer != nil {
	// 	dbQuerySp = tracer.StartSpan("db_query", opentracing.ChildOf(parentSpan.Context()))
	// }

	rows, err := s.db.Query(query)
	if err != nil {
		return nil, err
	}

	columns, _ := rows.Columns()

	count := len(columns)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)

	resultCh := make(chan map[string]interface{})

	go func() {
		defer rows.Close()
		defer close(resultCh)
		for rows.Next() {
			for i := range columns {
				valuePtrs[i] = &values[i]
			}

			rows.Scan(valuePtrs...)

			row := make(map[string]interface{})
			for i, col := range values {
				if col != nil {
					row[columns[i]] = col
				}
			}

			for i, col := range columns {
				val := values[i]

				b, ok := val.([]byte)
				var v interface{}
				if ok {
					v = string(b)
				} else {
					v = val
				}
				row[col] = v
			}

			resultCh <- row
		}
	}()

	return resultCh, err

	// // tracing
	// if dbQuerySp != nil {
	// 	dbQuerySp.Finish()
	// }
}

// Cleanup closes the connection to the MySQL instance
func (s *MySQLStateBackend) Cleanup() {
	if s.db != nil {
		s.db.Close()
	}
}

// SeparateTS ...
func (s *MySQLStateBackend) SeparateTS(table string) error {
	if s.logTimestamps {
		query := fmt.Sprintf("INSERT INTO %s_write_log (ts_local) VALUES (?)", table)
		stmtInsert, err := s.db.Prepare(query)
		if err != nil {
			return err
		}
		defer stmtInsert.Close()
		_, err = stmtInsert.Exec(time.Now())

		return err
	}
	return nil
}

// LogQuery ...
func (s *MySQLStateBackend) LogQuery(table string, ts time.Time, records []*pb.QueryRespRecord) error {
	if s.logTimestamps {
		go func() {
			rowID := ""
			for _, rec := range records {
				rowID += rec.RecordId + "|"
			}
			rowID = rowID[:len(rowID)-1]
			query := fmt.Sprintf("INSERT INTO %s_query_log (row_ids, ts_local) VALUES (?,?)", table)
			stmtInsert, err := s.db.Prepare(query)
			if err != nil {
				log.Fatal(err)
			}
			defer stmtInsert.Close()
			_, err = stmtInsert.Exec(rowID, ts)
			if err != nil {
				log.Fatal(err)
			}
		}()
	}
	return nil
}
