package queryengine

import (
	"database/sql"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/dvasilas/proteus-lobsters-bench/internal/datastore"
	proteusclient "github.com/dvasilas/proteus/pkg/proteus-go-client"
)

// QueryEngine ...
type QueryEngine interface {
	Query(limit int) (interface{}, error)
	Close()
}

// ProteusQueryEngine ...
type ProteusQueryEngine struct {
	proteusClient *proteusclient.Client
	projection    []string
}

// --------------------- Proteus --------------------

// NewProteusQueryEngine ...
func NewProteusQueryEngine(tracing bool) (ProteusQueryEngine, error) {
	address := "127.0.0.1:50350"
	for {
		c, err := net.DialTimeout("tcp", address, time.Duration(time.Second))
		if err != nil {
			time.Sleep(2 * time.Second)
			fmt.Println("retying connecting to: ", address)
		} else {
			c.Close()
			break
		}
	}

	c, err := proteusclient.NewClient(proteusclient.Host{Name: "127.0.0.1", Port: 50350}, tracing)
	if err != nil {
		return ProteusQueryEngine{}, err
	}

	err = errors.New("not tried yet")
	for err != nil {
		// _, err = c.QueryInternal("stateTableJoin", nil, nil, int64(1), nil, false)
		_, err = c.Query("SELECT title, description, short_id, user_id, vote_sum FROM qpu ORDER BY vote_sum DESC LIMIT 5")
		time.Sleep(2 * time.Second)
		fmt.Println("retying a test query", err)
	}

	return ProteusQueryEngine{
		proteusClient: c,
		projection:    []string{"title", "description", "short_id", "user_id", "vote_sum"},
	}, nil
}

// Query ...
func (qe ProteusQueryEngine) Query(limit int) (resp interface{}, err error) {
	// resp, err := qe.proteusClient.QueryInternal("stateTableJoin", nil, nil, int64(limit), nil, false)

	// resp, err := qe.proteusClient.QueryNoOp()

	resp, err = qe.proteusClient.Query("SELECT title, description, short_id, user_id, vote_sum FROM qpu ORDER BY vote_sum DESC LIMIT 5")

	// resp, err := qe.proteusClient.QueryArgs()
	if err != nil {
		return nil, err
	}

	return resp, nil
}

// Close ...
func (qe ProteusQueryEngine) Close() {
	qe.proteusClient.Close()
}

// ------------------ MySQL (with MVs) ---------------

// MySQLWithViewsQE ...
type MySQLWithViewsQE struct {
	ds *datastore.Datastore
}

// NewMySQLWithViewsQE ...
func NewMySQLWithViewsQE(ds *datastore.Datastore) MySQLWithViewsQE {
	return MySQLWithViewsQE{
		ds: ds,
	}
}

// Query ...
func (qe MySQLWithViewsQE) Query(limit int) (interface{}, error) {
	projection := []string{"title", "description", "short_id", "vote_count"}

	query := fmt.Sprintf("SELECT title, description, short_id, vote_count "+
		"FROM stories "+
		"ORDER BY vote_count DESC "+
		"LIMIT %d",
		limit)
	rows, err := qe.ds.Db.Query(query)
	if err != nil {
		return nil, err
	}

	values := make([]sql.RawBytes, len(projection))
	scanArgs := make([]interface{}, len(projection))
	result := make([]map[string]string, 0)
	for i := range values {
		scanArgs[i] = &values[i]
	}
	defer rows.Close()
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

// Close ...
func (qe MySQLWithViewsQE) Close() {
	qe.ds.Db.Close()
}

// ------------------ MySQL (no MVs) -----------------

// MySQLPlainQE ...
type MySQLPlainQE struct {
	ds *datastore.Datastore
}

// NewMySQLPlainQE ...
func NewMySQLPlainQE(ds *datastore.Datastore) MySQLPlainQE {
	return MySQLPlainQE{
		ds: ds,
	}
}

// Query ...
func (qe MySQLPlainQE) Query(limit int) (interface{}, error) {
	projection := []string{"story_id", "title", "description", "short_id", "vote_count"}

	query := fmt.Sprintf("SELECT story_id, s.title, s.description, s.short_id, vote_count "+
		"FROM stories s "+
		"JOIN ( "+
		"SELECT v.story_id, SUM(v.vote) as vote_count "+
		"FROM votes v "+
		"WHERE v.comment_id IS NULL "+
		"GROUP BY v.story_id) "+
		"vc ON s.id = vc.story_id "+
		"ORDER BY vote_count DESC "+
		"LIMIT %d",
		limit)

	rows, err := qe.ds.Db.Query(query)
	if err != nil {
		return nil, err
	}

	values := make([]sql.RawBytes, len(projection))
	scanArgs := make([]interface{}, len(projection))
	result := make([]map[string]string, 0)
	for i := range values {
		scanArgs[i] = &values[i]
	}
	defer rows.Close()
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

// Close ...
func (qe MySQLPlainQE) Close() {
	qe.ds.Db.Close()
}
