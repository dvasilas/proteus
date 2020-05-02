package main

import (
	"errors"
	"flag"
	"fmt"
	"path/filepath"
	"runtime"

	"github.com/dvasilas/proteus/src/config"
	"github.com/dvasilas/proteus/src/qpu/server"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func main() {
	initDebug()

	var configFile string
	flag.StringVar(&configFile, "config", "noArg", "configuration file")
	debug := flag.Bool("d", false, "show debug messages")
	flag.Parse()

	if *debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	var conf config.ConfJSON
	err := readConfigFile(configFile, &conf)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("config read from file", conf)

	err = server.Server(conf)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("QPU server failed")
	}
}

//func readConfigFile(file string, conf *DSQPUConfig) error {
func readConfigFile(file string, conf interface{}) error {
	_, f, _, _ := runtime.Caller(0)
	basepath := filepath.Dir(f)
	viper.SetConfigName(file)
	viper.AddConfigPath(basepath + "/")
	viper.AddConfigPath(basepath + "/local")
	viper.AddConfigPath(basepath + "/dockerCompose")
	viper.SetConfigType("json")
	if err := viper.ReadInConfig(); err != nil {
		return err
	}
	if err := viper.Unmarshal(conf); err != nil {
		return err
	}
	return nil
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
