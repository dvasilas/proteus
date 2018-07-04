package main

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/abiosoft/ishell"
	pbQPU "github.com/dimitriosvasilas/modqp/protos"
	cli "github.com/dimitriosvasilas/modqp/scanQPU/client"
)

func queryConsumer(msg chan *pbQPU.Object, done chan bool) {
	for {
		if doneMsg := <-done; doneMsg {
			return
		}
		result := <-msg
		fmt.Println(result)
	}
}

func main() {
	shell := ishell.New()

	c, conn := cli.NewClient("localhost:50053")
	defer conn.Close()

	shell.Println("Data Store QPU Shell")

	shell.AddCmd(&ishell.Cmd{
		Name: "find",
		Help: "Perform a query on object attribute",
		Func: func(ctx *ishell.Context) {
			if len(ctx.Args) < 3 {
				ctx.Err(errors.New("missing argument(s)"))
				return
			}
			lbound, err := strconv.ParseInt(ctx.Args[1], 10, 64)
			if err != nil {
				ctx.Err(errors.New("find lower bound is not int"))
				return
			}
			ubound, err := strconv.ParseInt(ctx.Args[2], 10, 64)
			if err != nil {
				ctx.Err(errors.New("find upper bound is not int"))
				return
			}

			msg := make(chan *pbQPU.Object)
			done := make(chan bool)
			query := map[string][2]int64{ctx.Args[0]: [2]int64{lbound, ubound}}

			ctx.ProgressBar().Indeterminate(true)
			ctx.ProgressBar().Start()

			go c.Find(time.Now().UnixNano(), query, msg, done)
			queryConsumer(msg, done)

			ctx.ProgressBar().Stop()
		},
	})

	shell.Run()
}
