package main

import (
	"fmt"
	"log"

	"github.com/dvasilas/proteus/internal/proto/qpu"
	proteusclient "github.com/dvasilas/proteus/pkg/proteus-go-client"
)

type benchmark struct {
	attributesToPrint []string
	proteusClient     *proteusclient.Client
}

func main() {
	c, err := proteusclient.NewClient(proteusclient.Host{Name: "127.0.0.1", Port: 50350})
	if err != nil {
		log.Fatal(err)
	}

	bench := &benchmark{
		attributesToPrint: []string{"title", "description", "short_id", "user_id", "vote_sum"},
		proteusClient:     c,
	}

	bench.doQuery("stateTableJoin", nil, nil, 0, nil, false)
}

func (b *benchmark) doQuery(table string, predicate []*qpu.AttributePredicate, ts *qpu.SnapshotTimePredicate, limit int64, md map[string]string, sync bool) {
	resp, err := b.proteusClient.QueryInternal(table, predicate, ts, limit, md, sync)
	if err != nil {
		log.Fatal(err)
	}

	for _, entry := range resp {
		b.processResponseRecord(entry)
	}
}

func (b *benchmark) processResponseRecord(respRecord proteusclient.ResponseRecord) {
	// fmt.Printf("%s: ( ", respRecord.ObjectID)

	for i, attributeKey := range b.attributesToPrint {
		if val, found := respRecord.State[attributeKey]; found {
			fmt.Printf("%s: %s", attributeKey, val)
			if i < len(b.attributesToPrint)-1 {
				fmt.Printf(", ")
			}
		} else {
			log.Fatal("attribute not found")
		}
	}

	fmt.Printf(" \n")
}
