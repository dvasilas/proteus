package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"text/tabwriter"

	"github.com/dvasilas/proteus/src/config"
	"github.com/dvasilas/proteus/src/qpu/server"
	"github.com/pelletier/go-toml"
	log "github.com/sirupsen/logrus"
)

func main() {
	var configFile string
	flag.StringVar(&configFile, "c", "noArg", "configuration file")
	debug := flag.Bool("d", false, "show debug messages")

	flag.Usage = func() {
		fmt.Fprintln(os.Stdout, "usage:  -c config_file [-d]")
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

	if *debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	conf := config.ConfJSON{}
	err := readConfigFile(configFile, &conf)
	if err != nil {
		log.Fatal(err)
	}

	err = server.Server(conf)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("QPU server failed")
	}
}

func readConfigFile(configFile string, conf *config.ConfJSON) error {
	configData, err := ioutil.ReadFile(configFile)
	if err != nil {
		log.Fatal(err)
	}

	toml.Unmarshal(configData, conf)

	return nil
}
