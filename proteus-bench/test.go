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
	/*
		for i := 0; i < 10; i++ {
			go func() {
				for {
					query(c, []proteusclient.AttributePredicate{proteusclient.AttributePredicate{AttrName: "test", AttrType: proteusclient.S3TAGFLT, Lbound: 0.1, Ubound: 0.3}}, proteusclient.LATESTSNAPSHOT)
					query(c, []proteusclient.AttributePredicate{proteusclient.AttributePredicate{AttrName: "test", AttrType: proteusclient.S3TAGFLT, Lbound: 0.3, Ubound: 0.5}}, proteusclient.LATESTSNAPSHOT)
					query(c, []proteusclient.AttributePredicate{proteusclient.AttributePredicate{AttrName: "test", AttrType: proteusclient.S3TAGFLT, Lbound: 0.5, Ubound: 0.7}}, proteusclient.LATESTSNAPSHOT)
					query(c, []proteusclient.AttributePredicate{proteusclient.AttributePredicate{AttrName: "test", AttrType: proteusclient.S3TAGFLT, Lbound: 0.1, Ubound: 0.5}}, proteusclient.LATESTSNAPSHOT)
					query(c, []proteusclient.AttributePredicate{proteusclient.AttributePredicate{AttrName: "test", AttrType: proteusclient.S3TAGFLT, Lbound: 0.1, Ubound: 0.7}}, proteusclient.LATESTSNAPSHOT)
					time.Sleep(time.Millisecond * 100)
				}
			}()
		}
	*/
	//for {
	query(c, []proteusclient.AttributePredicate{
		// proteusclient.AttributePredicate{AttrName: "votes_cool", AttrType: proteusclient.S3TAGINT, Lbound: int64(1), Ubound: int64(2)},
		proteusclient.AttributePredicate{AttrName: "trip_distance", AttrType: proteusclient.S3TAGFLT, Lbound: 1.60, Ubound: 2.60},
	}, proteusclient.LATESTSNAPSHOT)
	// query(c, []proteusclient.AttributePredicate{proteusclient.AttributePredicate{AttrName: "test", AttrType: proteusclient.CRDTCOUNTER, Lbound: int64(0), Ubound: int64(100)}}, proteusclient.LATESTSNAPSHOT)

	//query(c, []proteusclient.AttributePredicate{proteusclient.AttributePredicate{AttrName: "test", AttrType: proteusclient.S3TAGFLT, Lbound: 0.3, Ubound: 0.5}}, proteusclient.LATESTSNAPSHOT)
	//query(c, []proteusclient.AttributePredicate{proteusclient.AttributePredicate{AttrName: "test", AttrType: proteusclient.S3TAGFLT, Lbound: 0.5, Ubound: 0.7}}, proteusclient.LATESTSNAPSHOT)
	//query(c, []proteusclient.AttributePredicate{proteusclient.AttributePredicate{AttrName: "test", AttrType: proteusclient.S3TAGFLT, Lbound: 0.1, Ubound: 0.5}}, proteusclient.LATESTSNAPSHOT)
	//query(c, []proteusclient.AttributePredicate{proteusclient.AttributePredicate{AttrName: "test", AttrType: proteusclient.S3TAGFLT, Lbound: 0.1, Ubound: 0.7}}, proteusclient.LATESTSNAPSHOT)
	//time.Sleep(time.Millisecond * 100)
	//query(c, []proteusclient.AttributePredicate{proteusclient.AttributePredicate{AttrName: "test", AttrType: proteusclient.S3TAGFLT, Lbound: 0.1, Ubound: 0.7}}, proteusclient.NOTIFY)

	/*
		cmd := exec.Command("s3cmd", "put", "--host=127.0.0.1:8000", "--add-header=x-amz-meta-f-test:0.5", "/Users/dimvas/go/src/github.com/dvasilas/proteus/test/test.go", "s3://local-s3/obj3")
		var out bytes.Buffer
		cmd.Stdout = &out
		err = cmd.Run()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("in all caps: %q\n", out.String())
	*/
}

//}

func query(c *proteusclient.Client, pred []proteusclient.AttributePredicate, qType proteusclient.QueryType) {
	queryMetadata := map[string]string{"maxResponseCount": "3"}
	respCh, errCh, err := c.Query(pred, qType, queryMetadata)
	// respCh, errCh, err := c.Query(pred, qType, nil)
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
