package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"text/tabwriter"
	"time"

	"github.com/dvasilas/proteus/pkg/libbench"
	log "github.com/sirupsen/logrus"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	log.SetLevel(log.TraceLevel)

	var configFile, system string
	flag.StringVar(&configFile, "c", "noArg", "configuration file")
	flag.StringVar(&system, "s", "noArg", "system to be used as the query engine")
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

	if len(os.Args) < 3 {
		flag.Usage()
		return
	}

	flag.Parse()

	if system == "noArg" || configFile == "noArg" {
		flag.Usage()
		return
	}

	bench, err := libbench.NewBenchmark(configFile, system)
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

	err = bench.GetHomepage()
	if err != nil {
		log.Fatal(err)
	}

}
