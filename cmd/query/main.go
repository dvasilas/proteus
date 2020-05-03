package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/dvasilas/proteus/proteus_client"
)

func main() {
	var endpoint, query string
	flag.StringVar(&endpoint, "e", "noArg", "QPU endpoint")
	flag.StringVar(&query, "q", "noArg", "query")

	flag.Usage = func() {
		fmt.Fprintln(os.Stdout, "usage:  -e qpu_endpoint -q query")
		w := new(tabwriter.Writer)
		w.Init(os.Stdout, 0, 10, 0, '\t', 0)
		flag.VisitAll(func(f *flag.Flag) {
			fmt.Fprintf(w, "  -%v\t%v\n", f.Name, f.Usage)
		})
		w.Flush()
	}

	if len(os.Args) < 4 {
		flag.Usage()
		return
	}

	flag.Parse()

	connection := strings.Split(endpoint, ":")
	port, err := strconv.ParseInt(connection[1], 10, 64)
	if err != nil {
		fmt.Println(err)
		return
	}
	c, err := proteusclient.NewClient(proteusclient.Host{Name: connection[0], Port: int(port)})
	if err != nil {
		log.Fatal(err)
	}
	doQuery(c, query)
}

func doQuery(c *proteusclient.Client, query string) {
	respCh, errCh, err := c.QuerySQL(query)
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
