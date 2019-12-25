package main

import (
	"flag"
	"fmt"
	"io"
	"log"

	"github.com/dvasilas/proteus/proteus_client"
)

func main() {

	port := flag.Int("port", 0, "qpu port")
	flag.Parse()

	c, err := proteusclient.NewClient(proteusclient.Host{Name: "127.0.0.1", Port: *port})
	if err != nil {
		log.Fatal(err)
	}
	query(c, []proteusclient.AttributePredicate{
		proteusclient.AttributePredicate{AttrName: "tripdistance", AttrType: proteusclient.S3TAGFLT, Lbound: 0.00, Ubound: 10.0},
	}, proteusclient.LATESTSNAPSHOT)
}

func query(c *proteusclient.Client, pred []proteusclient.AttributePredicate, qType proteusclient.QueryType) {
	queryMetadata := map[string]string{"maxResponseCount": "3"}
	respCh, errCh, err := c.Query(pred, qType, queryMetadata)
	if err != nil {
		log.Fatal(err)
	}
	for {
		select {
		case err, ok := <-errCh:
			if !ok {
				errCh = nil
			} else if err == io.EOF {
			} else {
				log.Fatal(err)
			}
		case resp, ok := <-respCh:
			if !ok {
				respCh = nil
			} else {
				fmt.Println(resp)
			}
		}
		if errCh == nil && respCh == nil {
			fmt.Println("end of results")
			break
		}
	}
}
