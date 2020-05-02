package main

import (
	"flag"
	"io/ioutil"

	"github.com/dvasilas/proteus/src/config"
	"github.com/dvasilas/proteus/src/qpu/server"
	"github.com/pelletier/go-toml"
	log "github.com/sirupsen/logrus"
)

func main() {
	var configFile string
	flag.StringVar(&configFile, "config", "noArg", "configuration file")
	debug := flag.Bool("d", false, "show debug messages")
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
