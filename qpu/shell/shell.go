package main

import (
	"errors"
	"flag"
	"strings"
	"time"

	"github.com/abiosoft/ishell"
	utils "github.com/dimitriosvasilas/modqp"
	pb "github.com/dimitriosvasilas/modqp/protos/qpu"
	pbQPU "github.com/dimitriosvasilas/modqp/protos/utils"
	cli "github.com/dimitriosvasilas/modqp/qpu/client"
	log "github.com/sirupsen/logrus"
)

type query struct {
	datatype  string
	attribute string
	lbound    string
	ubound    string
}

func find(q []query, c cli.Client) error {
	var query map[string][2]*pbQPU.Value
	lbound, ubound, err := utils.AttrBoundStrToVal(q[0].datatype, q[0].lbound, q[0].ubound)
	if err != nil {
		return errors.New("bound error")
	}
	query = map[string][2]*pbQPU.Value{q[0].attribute: {lbound, ubound}}
	return sendQuery(query, c)
}

func sendQuery(query map[string][2]*pbQPU.Value, c cli.Client) error {
	msg := make(chan *pb.QueryResultStream)
	done := make(chan bool)
	errs := make(chan error)
	errs1 := make(chan error)

	go queryConsumer(query, msg, done, errs, errs1)
	c.Find(time.Now().UnixNano(), query, msg, done, errs)
	err := <-errs1
	return err
}

func queryConsumer(query map[string][2]*pbQPU.Value, msg chan *pb.QueryResultStream, done chan bool, errs chan error, errs1 chan error) {
	for {
		if doneMsg := <-done; doneMsg {
			err := <-errs
			errs1 <- err
		}
		res := <-msg
		displayResults(query, res.GetObject(), res.GetDataset())
	}
}

func displayResults(query map[string][2]*pbQPU.Value, obj *pbQPU.Object, ds *pbQPU.DataSet) {
	logMsg := log.Fields{
		"key":     obj.GetKey(),
		"dataset": ds,
	}
	for attr := range obj.GetAttributes() {
		for q := range query {
			if strings.Contains(attr, q) {
				switch obj.GetAttributes()[attr].Val.(type) {
				case *pbQPU.Value_Int:
					logMsg[q] = obj.GetAttributes()[attr].GetInt()
				case *pbQPU.Value_Flt:
					logMsg[q] = obj.GetAttributes()[attr].GetFlt()
				default:
					logMsg[q] = obj.GetAttributes()[attr].GetStr()
				}
			}
		}
	}
	log.WithFields(logMsg).Info("result")
}
func initShell(c cli.Client) {
	shell := ishell.New()
	shell.Println("QPU Shell")

	shell.AddCmd(&ishell.Cmd{
		Name: "find",
		Help: "Perform a query on object attribute",
		Func: func(ctx *ishell.Context) {
			query, err := processQueryString(ctx.Args[0])
			if err != nil {
				ctx.Err(err)
				return
			}
			err = find(query, c)
			if err != nil {
				ctx.Err(err)
				return
			}
		},
	})
	shell.Run()
}

func processQueryString(q string) ([]query, error) {
	queryProcessed := make([]query, 0)
	predicate := strings.Split(q, "&")

	for _, p := range predicate {
		datatype := strings.Split(p, "_")
		if len(datatype) < 2 {
			return nil, errors.New("Query should have the form predicate[&predicate], where predicate=type_attrKey=lbound/ubound")
		}
		attrK := strings.Split(datatype[1], "=")
		if len(attrK) < 2 {
			return nil, errors.New("Query should have the form predicate&[predicate], where predicate=type_attrKey=lbound/ubound")
		}
		bound := strings.Split(attrK[1], "/")
		if len(bound) < 2 {
			return nil, errors.New("Query should have the form predicate&[predicate], where predicate=type_attrKey=lbound/ubound")
		}
		queryProcessed = append(queryProcessed, query{
			datatype:  datatype[0],
			attribute: attrK[0],
			lbound:    bound[0],
			ubound:    bound[1],
		})
	}
	return queryProcessed, nil
}

func main() {
	var endpoint string
	flag.StringVar(&endpoint, "endpoint", "noEndpoint", "QPU endpoint to send query")
	var queryIn string
	flag.StringVar(&queryIn, "query", "emptyQuery", "Query string")
	var mode string
	flag.StringVar(&mode, "mode", "noMode", "Script execution mode: cmd(command) / sh(shell) / http(http server)")
	flag.Parse()
	if endpoint == "noEndpoint" || mode == "noMode" || (mode != "cmd" && mode != "sh" && mode != "http") {
		flag.Usage()
		return
	}
	if mode == "cmd" && queryIn == "emptyQuery" {
		flag.Usage()
		return
	}
	c, conn, err := cli.NewClient(endpoint)
	defer conn.Close()
	if err != nil {
		log.Fatal("failed to create Client %v", err)
	}
	if mode == "cmd" {
		query, err := processQueryString(queryIn)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Fatal("Find failed")
		}
		err = find(query, c)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Fatal("Find failed")
		}
	} else if mode == "sh" {
		initShell(c)
	} else if mode == "http" {
		log.WithFields(log.Fields{
			"error": errors.New("Not implemented"),
		}).Fatal("Find failed")
	}
}
