package main

import (
	"flag"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/dvasilas/proteus/pkg/libbench"
	log "github.com/sirupsen/logrus"
)

func main() {

	// log.SetLevel(log.TraceLevel)

	var configFile, system string
	var threads int
	flag.StringVar(&configFile, "c", "noArg", "configuration file")
	flag.StringVar(&system, "s", "noArg", "system to be used as the query engine")
	flag.IntVar(&threads, "t", 1, "number of client threads to be used")
	preload := flag.Bool("p", false, "preload")

	flag.Usage = func() {
		fmt.Fprintln(os.Stdout, "usage:  -c config_file -s system [-p]")
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

	if !(*preload) && system == "noArg" {
		flag.Usage()
		return
	}

	bench, err := libbench.NewBenchmark(configFile, system, *preload)
	if err != nil {
		log.Fatal(err)
	}

	if *preload {
		err := bench.Preload()
		if err != nil {
			log.Fatal(err)
		}
		return
	}

	err = bench.Run(threads)
	if err != nil {
		log.Fatal(err)
	}

	bench.PrintMeasurements()

}
