package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"text/tabwriter"

	qpu "github.com/dvasilas/proteus/internal"
	log "github.com/sirupsen/logrus"
)

// This package implements the the top-level functionality of the qpu
// application.
// It is responsible for:
// - parsing the command line arguments give to the executable,
// - initializing the logging level,
// - installing a hook for executing a shutdown function to execute when the
//   process receives SIGINT,
// - and finally creating and starting the QPU service.

func main() {
	fmt.Println("GOMAXPROCS= ", runtime.GOMAXPROCS(runtime.NumCPU()))

	configFile, logLevel, err := getFlags()
	if err != nil {
		log.Fatal("qpuService.NewQPUService() failed: ", err)
	}
	err = setLoggingLevel(logLevel)
	if err != nil {
		log.Fatal("qpuService.NewQPUService() failed: ", err)
	}

	qpuService, err := qpu.NewQPUService(configFile)
	if err != nil {
		log.Fatal("qpuService.NewQPUService() failed: ", err)
	}

	setCleanup(qpuService.Shutdown)

	err = qpuService.Start()
	if err != nil {
		log.Fatal("qpuService.Start() failed: ", err)
	}
}

// ---------------- Internal Functions --------------

func getFlags() (string, string, error) {
	flag.Usage = func() {
		fmt.Fprintln(os.Stdout, "usage:  -c config_file [-l log_level]")
		w := new(tabwriter.Writer)
		w.Init(os.Stdout, 0, 10, 0, '\t', 0)
		flag.VisitAll(func(f *flag.Flag) {
			fmt.Fprintf(w, "  -%v\t%v\n", f.Name, f.Usage)
		})
		w.Flush()
	}

	var configFile, logLevel string
	flag.StringVar(&configFile, "c", "noArg", "configuration file")
	flag.StringVar(&logLevel, "l", "info", "logging level")

	if len(os.Args) < 2 {
		flag.Usage()
		return "", "", errors.New("not enough arguments given")
	}
	flag.Parse()

	return configFile, logLevel, nil
}

func setLoggingLevel(logLevel string) error {
	switch logLevel {
	case "trace":
		log.SetLevel(log.TraceLevel)
		return nil
	case "debug":
		log.SetLevel(log.DebugLevel)
		return nil
	case "info":
		log.SetLevel(log.InfoLevel)
		return nil
	case "warn":
		log.SetLevel(log.WarnLevel)
		return nil
	case "error":
		log.SetLevel(log.ErrorLevel)
		return nil
	case "fatal":
		log.SetLevel(log.FatalLevel)
		return nil
	case "panic":
		log.SetLevel(log.PanicLevel)
		return nil
	default:
		return errors.New("unexpected logging level. Options are trace, debug, info, warn, error, fatal, and panic")
	}
}

func setCleanup(cleanupFunc func()) {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		cleanupFunc()
		os.Exit(0)
	}()
}
