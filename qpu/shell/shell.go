package main

import (
	"errors"
	"os"
	"strings"
	"time"

	"github.com/abiosoft/ishell"
	utils "github.com/dimitriosvasilas/modqp"
	pb "github.com/dimitriosvasilas/modqp/protos/qpu"
	pbQPU "github.com/dimitriosvasilas/modqp/protos/utils"
	cli "github.com/dimitriosvasilas/modqp/qpu/client"
	log "github.com/sirupsen/logrus"
)

func find(datatype string, attr string, lb string, ub string, c cli.Client, errs chan error) {
	var query map[string][2]*pbQPU.Value
	if datatype == "int" {
		lbound, ubound, err := utils.AttrBoundStrToVal("int", lb, ub)
		if err != nil {
			errs <- errors.New("bound error")
			return
		}
		query = map[string][2]*pbQPU.Value{attr: {lbound, ubound}}
	} else if datatype == "str" {
		query = map[string][2]*pbQPU.Value{attr: {utils.ValStr(lb), utils.ValStr(ub)}}
	} else if datatype == "float" {
		lbound, ubound, err := utils.AttrBoundStrToVal("float", lb, ub)
		if err != nil {
			errs <- errors.New("bound error")
			return
		}
		query = map[string][2]*pbQPU.Value{attr: {lbound, ubound}}
	} else {
		errs <- errors.New("Unknown datatype")
		return
	}
	errs <- sendQuery(query, c)
}

func sendQuery(query map[string][2]*pbQPU.Value, c cli.Client) error {
	msg := make(chan *pb.QueryResultStream)
	done := make(chan bool)
	errs := make(chan error)

	go c.Find(time.Now().UnixNano(), query, msg, done, errs)
	return queryConsumer(query, msg, done, errs)
}

func queryConsumer(query map[string][2]*pbQPU.Value, msg chan *pb.QueryResultStream, done chan bool, errs chan error) error {
	for {
		if doneMsg := <-done; doneMsg {
			err := <-errs
			return err
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
	log.WithFields(logMsg).Infof("result")
}

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("No port provided")
		return
	}
	var port = os.Args[1]
	errs := make(chan error)

	c, conn, err := cli.NewClient("localhost:" + port)
	defer conn.Close()
	if err != nil {
		log.Fatalf("failed to create Client %v", err)
	}

	if len(os.Args) > 2 {
		if len(os.Args) == 4 {
			os.Args = append(os.Args, os.Args[3])
		}

		go find(os.Args[2], os.Args[3], os.Args[4], os.Args[5], c, errs)
		err := <-errs
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Fatalf("Find failed")
		}
	} else {
		shell := ishell.New()
		shell.Println("QPU Shell")

		shell.AddCmd(&ishell.Cmd{
			Name: "find",
			Help: "Perform a query on object attribute",
			Func: func(ctx *ishell.Context) {
				if len(ctx.Args) < 2 {
					ctx.Err(errors.New("missing argument(s)"))
					return
				}
				ctx.ProgressBar().Indeterminate(true)
				ctx.ProgressBar().Start()

				if len(ctx.Args) == 2 {
					ctx.Args = append(ctx.Args, ctx.Args[1])
				}
				go find(ctx.Args[0], ctx.Args[1], ctx.Args[2], ctx.Args[3], c, errs)
				err := <-errs
				if err != nil {
					ctx.Err(err)
					return
				}

				ctx.ProgressBar().Stop()
			},
		})
		shell.Run()
	}
}
