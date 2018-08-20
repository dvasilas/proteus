package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/abiosoft/ishell"
	utils "github.com/dimitriosvasilas/modqp"
	cli "github.com/dimitriosvasilas/modqp/qpu/client"
	pbQPU "github.com/dimitriosvasilas/modqp/qpuUtilspb"
)

func queryConsumer(msg chan *pbQPU.Object, done chan bool) {
	for {
		if doneMsg := <-done; doneMsg {
			return
		}
		res := <-msg
		fmt.Println(res.Key, "size:", res.Attributes["size"].GetInt())
	}
}

func main() {
	shell := ishell.New()

	c, conn, err := cli.NewClient("localhost:" + os.Args[1])
	defer conn.Close()
	if err != nil {
		log.Fatalf("failed to create Client %v", err)
	}

	shell.Println("QPU Shell")

	shell.AddCmd(&ishell.Cmd{
		Name: "find",
		Help: "Perform a query on object attribute",
		Func: func(ctx *ishell.Context) {
			if len(ctx.Args) < 2 {
				ctx.Err(errors.New("missing argument(s)"))
				return
			}
			var query map[string][2]*pbQPU.Value
			if ctx.Args[0] == "size" {
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
				query = map[string][2]*pbQPU.Value{ctx.Args[0]: [2]*pbQPU.Value{utils.ValInt(lbound), utils.ValInt(ubound)}}
			} else if ctx.Args[0] == "key" {
				query = map[string][2]*pbQPU.Value{ctx.Args[0]: [2]*pbQPU.Value{utils.ValStr(ctx.Args[1]), utils.ValStr(ctx.Args[1])}}
			}

			msg := make(chan *pbQPU.Object)
			done := make(chan bool)

			ctx.ProgressBar().Indeterminate(true)
			ctx.ProgressBar().Start()

			go c.Find(time.Now().UnixNano(), query, msg, done)
			queryConsumer(msg, done)

			ctx.ProgressBar().Stop()
		},
	})

	shell.Run()
}
