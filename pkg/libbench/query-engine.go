package libbench

import (
	"database/sql"
	"fmt"

	proteusclient "github.com/dvasilas/proteus/pkg/proteus-go-client"
)

type proteusQueryEngine struct {
	proteusClient *proteusclient.Client
	projection    []string
}

func newProteusQueryEngine() (proteusQueryEngine, error) {
	c, err := proteusclient.NewClient(proteusclient.Host{Name: "127.0.0.1", Port: 50350})
	if err != nil {
		return proteusQueryEngine{}, err
	}

	return proteusQueryEngine{
		proteusClient: c,
		projection:    []string{"title", "description", "short_id", "user_id", "vote_sum"},
	}, nil
}

func (qe proteusQueryEngine) query(table string) (interface{}, error) {
	resp, err := qe.proteusClient.QueryInternal(table, nil, nil, 5, nil, false)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

type mySQLQueryEngine struct {
	ds *datastore
}

func newMySQLQueryEngine(ds *datastore) (mySQLQueryEngine, error) {
	return mySQLQueryEngine{
		ds: ds,
	}, nil
}

func (qe mySQLQueryEngine) query(table string) (interface{}, error) {
	projection := []string{"title", "description", "short_id", "user_id", "vote_count", "unix_timestamp(ts)"}

	projectionStmt := ""
	for i, attr := range projection {
		projectionStmt += attr
		if i < len(projection)-1 {
			projectionStmt += ", "
		}
	}

	query := fmt.Sprintf("SELECT %s FROM %s %s",
		projectionStmt,
		"stories_with_votecount",
		"LIMIT 2")
	rows, err := qe.ds.db.Query(query)
	if err != nil {
		return nil, err
	}

	values := make([]sql.RawBytes, len(projection))
	scanArgs := make([]interface{}, len(projection))
	result := make([]map[string]string, 0)
	for i := range values {
		scanArgs[i] = &values[i]
	}
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}
		row := make(map[string]string)
		for i, col := range values {
			if col != nil {
				row[projection[i]] = string(col)
			}
		}
		result = append(result, row)
	}

	return result, nil
}
