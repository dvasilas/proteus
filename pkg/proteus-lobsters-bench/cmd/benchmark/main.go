package main

import (
	"flag"
	"fmt"
	"os"
	"text/tabwriter"

	benchmark "github.com/dvasilas/proteus-lobsters-bench/internal"
	log "github.com/sirupsen/logrus"
)

func main() {

	// log.SetLevel(log.TraceLevel)

	var configFile string
	var threads int
	flag.StringVar(&configFile, "c", "noArg", "configuration file")
	flag.IntVar(&threads, "t", 0, "number of client threads to be used")
	preload := flag.Bool("p", false, "preload")
	dryRun := flag.Bool("d", false, "dryRun: print configuration and exit")

	flag.Usage = func() {
		fmt.Fprintln(os.Stdout, "usage: -c config_file -s system [-p]")
		w := new(tabwriter.Writer)
		w.Init(os.Stdout, 0, 10, 0, '\t', 0)
		flag.VisitAll(func(f *flag.Flag) {
			fmt.Fprintf(w, "  -%v\t%v\n", f.Name, f.Usage)
		})
		w.Flush()
	}

	if len(os.Args) < 2 {
		flag.Usage()
		return
	}

	flag.Parse()

	if configFile == "noArg" {
		flag.Usage()
		return
	}

	bench, err := benchmark.NewBenchmark(configFile, *preload, threads, *dryRun)
	if err != nil {
		log.Fatal(err)
	}

	if *dryRun {
		return
	}

	if *preload {
		err = bench.Preload()
		if err != nil {
			log.Fatal(err)
		}
		return
	}

	err = bench.Run()
	if err != nil {
		log.Fatal(err)
	}

	bench.PrintMeasurements()
}
