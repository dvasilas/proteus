package main

import (
	"errors"
	"os"
	"strconv"
	"time"

	"github.com/abiosoft/ishell"
	utils "github.com/dimitriosvasilas/modqp"
	cli "github.com/dimitriosvasilas/modqp/qpu/client"
	pbQPU "github.com/dimitriosvasilas/modqp/qpuUtilspb"
	log "github.com/sirupsen/logrus"
)

func queryConsumer(msg chan *pbQPU.Object, done chan bool, errs chan error) error {
	for {
		if doneMsg := <-done; doneMsg {
			err := <-errs
			return err
		}
		res := <-msg
		log.WithFields(log.Fields{
			"key":  res.Key,
			"size": res.Attributes["size"].GetInt(),
		}).Infof("result")
	}
}

func find(attr string, lb string, ub string, c cli.Client, errs chan error) {
	var query map[string][2]*pbQPU.Value
	if attr == "size" {
		lbound, err := strconv.ParseInt(lb, 10, 64)
		if err != nil {
			errs <- errors.New("Lower bound is not int")
			return
		}
		ubound, err := strconv.ParseInt(ub, 10, 64)
		if err != nil {
			errs <- errors.New("Upper bound is not int")
			return
		}
		query = map[string][2]*pbQPU.Value{attr: [2]*pbQPU.Value{utils.ValInt(lbound), utils.ValInt(ubound)}}
	} else if attr == "key" {
		query = map[string][2]*pbQPU.Value{attr: [2]*pbQPU.Value{utils.ValStr(lb), utils.ValStr(ub)}}
	}
	errs <- sendQuery(query, c)
}

func sendQuery(query map[string][2]*pbQPU.Value, c cli.Client) error {
	msg := make(chan *pbQPU.Object)
	done := make(chan bool)
	errs := make(chan error)

	go c.Find(time.Now().UnixNano(), query, msg, done, errs)
	return queryConsumer(msg, done, errs)
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

		go find(os.Args[2], os.Args[3], os.Args[4], c, errs)
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
				go find(ctx.Args[0], ctx.Args[1], ctx.Args[2], c, errs)
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
