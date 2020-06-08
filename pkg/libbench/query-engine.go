package libbench

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/dvasilas/proteus/internal/proto/qpu"
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

func (qe proteusQueryEngine) getHomepage() error {
	return qe.doQuery("stateTableJoin", nil, nil, 5, nil, false)
}

func (qe proteusQueryEngine) doQuery(table string, predicate []*qpu.AttributePredicate, ts *qpu.SnapshotTimePredicate, limit int64, md map[string]string, sync bool) error {
	resp, err := qe.proteusClient.QueryInternal(table, predicate, ts, limit, md, sync)
	if err != nil {
		return err
	}

	for _, entry := range resp {
		err = qe.processResponseRecord(entry)
		if err != nil {
			return err
		}
	}

	return nil
}

func (qe proteusQueryEngine) processResponseRecord(respRecord proteusclient.ResponseRecord) error {
	for i, attributeKey := range qe.projection {
		if val, found := respRecord.State[attributeKey]; found {
			fmt.Printf("%s: %s", attributeKey, val)
			if i < len(qe.projection)-1 {
				fmt.Printf(", ")
			}
		} else {
			log.Fatal("attribute not found")
		}
	}
	fmt.Printf(" \n")

	return nil
}

type mySQLQueryEngine struct {
	ds *datastore
}

func newMySQLQueryEngine(ds *datastore) (mySQLQueryEngine, error) {
	return mySQLQueryEngine{
		ds: ds,
	}, nil
}

func (qe mySQLQueryEngine) getHomepage() error {
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
		return err
	}
	values := make([]sql.RawBytes, len(projection))
	scanArgs := make([]interface{}, len(projection))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return err
		}
		row := make(map[string]string)
		for i, col := range values {
			if col != nil {
				row[projection[i]] = string(col)
			}
		}
		fmt.Println(row)
	}
	return nil
}
