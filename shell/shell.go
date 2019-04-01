package main

import (
	"errors"
	"flag"
	"strings"
	"time"

	"github.com/abiosoft/ishell"
	"github.com/dvasilas/proteus"
	attribute "github.com/dvasilas/proteus/attributes"
	pb "github.com/dvasilas/proteus/protos/qpu"
	pbQPU "github.com/dvasilas/proteus/protos/utils"
	cli "github.com/dvasilas/proteus/qpu/client"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

type shell struct {
	flags  flags
	client cli.Client
	conn   *grpc.ClientConn
}

type query struct {
	datatype  string
	attribute string
	lbound    string
	ubound    string
}

type flags struct {
	endpoint string
	queryIn  string
	mode     string
}

func (sh *shell) find(q []query) error {
	log.Debug("shell:find ", q)
	var query []*pbQPU.AttributePredicate

	attr, _, err := attribute.Attr(q[0].attribute, nil)
	if err != nil {
		return err
	}
	lbound, ubound, err := attr.BoundStrToVal(q[0].lbound, q[0].ubound)
	if err != nil {
		return errors.New("bound error")
	}
	query = append(query, &pbQPU.AttributePredicate{
		Attribute: attr.GetKey(q[0].attribute),
		Datatype:  attr.GetDatatype(),
		Lbound:    lbound,
		Ubound:    ubound,
	})
	log.Debug("shell:find ", query)

	return sh.sendQuery(query)
}

func (sh *shell) sendQuery(pred []*pbQPU.AttributePredicate) error {
	log.Debug("shell:sendQuery ", pred)

	errs := make(chan error)

	streamIn, _, err := sh.client.Find(&pbQPU.TimestampPredicate{Lbound: &pbQPU.Timestamp{Ts: time.Now().UnixNano()}}, pred)
	go utils.FindResponseConsumer(pred, streamIn, nil, errs, displayResponse)
	err = <-errs
	return err
}

func displayResponse(pred []*pbQPU.AttributePredicate, streamMsg *pb.FindResponseStream, stream pb.QPU_FindServer) error {
	log.Debug("shell:queryConsumer received: ", streamMsg)
	return printResponse(pred, streamMsg.GetObject(), streamMsg.GetDataset())
}

func (sh *shell) processQueryString(q string) ([]query, error) {
	log.Debug("shell:processQueryString: ", q)

	queryProcessed := make([]query, 0)
	predicate := strings.Split(q, "&")

	for _, p := range predicate {
		attrK := strings.Split(p, "=")
		if len(attrK) < 2 {
			return nil, errors.New("Query should have the form predicate&[predicate], where predicate=type_attrKey=lbound/ubound")
		}
		bound := strings.Split(attrK[1], "/")
		if len(bound) < 2 {
			return nil, errors.New("Query should have the form predicate&[predicate], where predicate=type_attrKey=lbound/ubound")
		}
		attr, _, err := attribute.Attr(attrK[0], nil)
		if err != nil {
			return nil, err
		}
		queryProcessed = append(queryProcessed, query{
			datatype:  attr.GetDatatype(),
			attribute: attrK[0],
			lbound:    bound[0],
			ubound:    bound[1],
		})
	}

	log.Debug("shell:processQueryString: ", queryProcessed)

	return queryProcessed, nil
}

func main() {
	sh, err := newShell()
	defer sh.conn.Close()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("shell failed")
	}
}

func printResponse(query []*pbQPU.AttributePredicate, obj *pbQPU.Object, ds *pbQPU.DataSet) error {
	logMsg := log.Fields{
		"key":     obj.GetKey(),
		"dataset": ds,
	}
	if obj.Key != "noResults" {
		for _, p := range query {
			attr, _, err := attribute.Attr(p.Attribute, obj)
			if err != nil {
				return err
			}
			logMsg[p.Attribute] = attr.GetValue(p.Attribute, obj)
		}
	}
	log.WithFields(logMsg).Info("result")
	return nil
}

func newShell() (shell, error) {
	err := initDebug()
	if err != nil {
		return shell{}, err
	}
	flags, err := getFlags()
	if err != nil {
		return shell{}, err
	}
	client, conn, err := cli.NewClient(flags.endpoint)
	if err != nil {
		return shell{}, err
	}
	shell := shell{flags, client, conn}
	shell.initialize()
	return shell, nil
}

func (sh *shell) initialize() error {
	switch sh.flags.mode {
	case "cmd":
		query, err := sh.processQueryString(sh.flags.queryIn)
		if err != nil {
			return err
		}
		err = sh.find(query)
		if err != nil {
			return err
		}
	case "sh":
		sh.initInteractiveShell()
	case "http":
		return errors.New("Not implemented")
	default:
		return errors.New("unknown shell type")
	}
	return nil
}

func (sh *shell) initInteractiveShell() {
	shell := ishell.New()
	shell.Println("QPU Shell")

	shell.AddCmd(&ishell.Cmd{
		Name: "find",
		Help: "Perform a query on object attribute",
		Func: func(ctx *ishell.Context) {
			query, err := sh.processQueryString(ctx.Args[0])
			if err != nil {
				ctx.Err(err)
				return
			}
			err = sh.find(query)
			if err != nil {
				ctx.Err(err)
				return
			}
		},
	})
	shell.Run()
}

func getFlags() (flags, error) {
	var flags flags
	flag.StringVar(&flags.endpoint, "endpoint", "noEndpoint", "QPU endpoint to send query")
	flag.StringVar(&flags.queryIn, "query", "emptyQuery", "Query string")
	flag.StringVar(&flags.mode, "mode", "noMode", "Script execution mode: cmd(command) / sh(shell) / http(http server)")

	flag.Parse()
	if flags.endpoint == "noEndpoint" || flags.mode == "noMode" || (flags.mode != "cmd" && flags.mode != "sh" && flags.mode != "http") {
		flag.Usage()
		return flags, errors.New("flag error")
	}
	if flags.mode == "cmd" && flags.queryIn == "emptyQuery" {
		flag.Usage()
		return flags, errors.New("flag error")
	}
	return flags, nil
}

func initDebug() error {
	err := viper.BindEnv("DEBUG")
	if err != nil {
		return errors.New("BindEnv DEBUG failed")
	}
	debug := viper.GetBool("DEBUG")
	if debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}
	return nil
}
