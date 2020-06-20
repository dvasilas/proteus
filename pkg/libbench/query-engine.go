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

// --------------------- Proteus --------------------

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

func (qe proteusQueryEngine) query(limit int) (interface{}, error) {
	resp, err := qe.proteusClient.QueryInternal("stateTableJoin", nil, nil, int64(limit), nil, false)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

type mySQLWithViewsQE struct {
	ds *datastore
}

// ------------------ MySQL (with MVs) ---------------

func newMySQLWithViewsQE(ds *datastore) (mySQLWithViewsQE, error) {
	return mySQLWithViewsQE{
		ds: ds,
	}, nil
}

func (qe mySQLWithViewsQE) query(limit int) (interface{}, error) {
	projection := []string{"title", "description", "short_id", "vote_count"}

	query := fmt.Sprintf("SELECT title, description, short_id, vote_count "+
		"FROM stories "+
		"ORDER BY vote_count DESC "+
		"LIMIT %d",
		limit)
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

type mySQLPlainQE struct {
	ds *datastore
}

// ------------------ MySQL (no MVs) -----------------

func newMySQLPlainQE(ds *datastore) (mySQLPlainQE, error) {
	return mySQLPlainQE{
		ds: ds,
	}, nil
}

func (qe mySQLPlainQE) query(limit int) (interface{}, error) {
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
