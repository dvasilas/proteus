package main

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/abiosoft/ishell"
	cli "github.com/dimitriosvasilas/modqp/dataStoreQPU/client"
	pb "github.com/dimitriosvasilas/modqp/dataStoreQPU/dsqpu"
)

func queryConsumer(msg chan *pb.ObjectMD, done chan bool) {
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

	c, conn := cli.NewDSQPUClient("localhost:50052")
	defer conn.Close()

	shell.Println("Data Store QPU Shell")

	shell.AddCmd(&ishell.Cmd{
		Name: "query",
		Help: "Peform a query on object attribute",
		Func: func(ctx *ishell.Context) {
			if len(ctx.Args) < 3 {
				ctx.Err(errors.New("missing argument(s)"))
				return
			}
			lbound, err := strconv.ParseInt(ctx.Args[1], 10, 64)
			if err != nil {
				ctx.Err(errors.New("query lower bound is not int"))
				return
			}
			ubound, err := strconv.ParseInt(ctx.Args[2], 10, 64)
			if err != nil {
				ctx.Err(errors.New("query upper bound is not int"))
				return
			}

			msg := make(chan *pb.ObjectMD)
			done := make(chan bool)
			query := map[string][2]int64{ctx.Args[0]: [2]int64{lbound, ubound}}

			ctx.ProgressBar().Indeterminate(true)
			ctx.ProgressBar().Start()

			go c.Query(time.Now().UnixNano(), query, msg, done)
			queryConsumer(msg, done)

			ctx.ProgressBar().Stop()
		},
	})

	shell.Run()
}
