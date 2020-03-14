package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"

	"github.com/dvasilas/proteus/proteus_client"
)

func main() {

	port := flag.Int("port", 0, "qpu port")
	bucket := flag.String("bucket", "noArg", "bucket")
	flag.Parse()

	if *bucket == "noArg" {
		log.Fatal(errors.New("no bucket argument given"))
	}

	c, err := proteusclient.NewClient(proteusclient.Host{Name: "127.0.0.1", Port: *port})
	if err != nil {
		log.Fatal(err)
	}
	query(c, *bucket, []proteusclient.AttributePredicate{
		proteusclient.AttributePredicate{AttrName: "tripdistance", AttrType: proteusclient.S3TAGFLT, Lbound: 0.00, Ubound: 1000.0},
	}, proteusclient.LATESTSNAPSHOT)
}

func query(c *proteusclient.Client, bucket string, pred []proteusclient.AttributePredicate, qType proteusclient.QueryType) {
	queryMetadata := map[string]string{"maxResponseCount": "3"}
	respCh, errCh, err := c.Query(bucket, pred, qType, queryMetadata)
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
