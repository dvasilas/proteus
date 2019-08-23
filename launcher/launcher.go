package main

import (
	"errors"
	"flag"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/dvasilas/proteus/src/config"
	"github.com/dvasilas/proteus/src/qpu/server"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// DataStoreConfJSON ...
type DataStoreConfJSON struct {
	BucketName         string
	AwsAccessKeyID     string
	AwsSecretAccessKey string
}

// IndexConfJSON ...
type IndexConfJSON struct {
	AttributeName string
	AttributeType string
	LBound        string
	UBound        string
	ConsLevel     string
	IndexStore    struct {
		Store          string
		Endpoint       string
		Bucket         string
		Implementation string
	}
}

func main() {
	initDebug()

	genFlags := flag.NewFlagSet("general flags", flag.ExitOnError)
	var qType, port string
	genFlags.StringVar(&qType, "qpu", "noArg", "QPU type")
	genFlags.StringVar(&port, "port", "noArg", "port for incoming connections")
	genFlags.Parse(os.Args[1:3])

	conf := config.ConfJSON{
		QpuType: qType,
		Port:    port,
	}

	n := 3
	switch qType {
	case "dbdriver":
		dbFlags := flag.NewFlagSet("datastore flags", flag.ExitOnError)
		var dataset, dbtype, endpoints string
		dbFlags.StringVar(&dataset, "dataset", "noArg", "dataset (db, dc, shard ids)")
		dbFlags.StringVar(&dbtype, "db", "noArg", "datastore type")
		dbFlags.StringVar(&endpoints, "endp", "noArg", "endpoints for dataset iteration and update notifications")
		dbFlags.Parse(os.Args[3:6])

		dset := strings.Split(dataset, "/")
		endp := strings.Split(endpoints, "/")
		conf.DataStoreConfig.DataSet.DB = dset[0]
		conf.DataStoreConfig.DataSet.DC = dset[1]
		conf.DataStoreConfig.DataSet.Shard = dset[2]
		conf.DataStoreConfig.Type = dbtype
		if dbtype == "s3" {
			conf.DataStoreConfig.Endpoint = "http://" + endp[0]
		} else if dbtype == "antidote" {
			conf.DataStoreConfig.Endpoint = endp[0]
		}
		conf.DataStoreConfig.LogStreamEndpoint = endp[1]

		var dsConf DataStoreConfJSON
		err := readConfigFile(dbtype, &dsConf)
		if err != nil {
			log.Fatal(err)
		}
		conf.DataStoreConfig.BucketName = dsConf.BucketName
		if dbtype == "s3" {
			conf.DataStoreConfig.AwsAccessKeyID = dsConf.AwsAccessKeyID
			conf.DataStoreConfig.AwsSecretAccessKey = dsConf.AwsSecretAccessKey
		}
	case "cache":
		var size int
		cacheFlags := flag.NewFlagSet("cache QPU flags", flag.ExitOnError)
		cacheFlags.IntVar(&size, "Csize", 0, "the cache's size")
		cacheFlags.Parse(os.Args[3:4])
		n++
		conf.CacheConfig.Size = size
	case "index":
		var configFile string
		indexFlags := flag.NewFlagSet("index QPU flags", flag.ExitOnError)
		indexFlags.StringVar(&configFile, "config", "noArg", "the index configuration file")
		indexFlags.Parse(os.Args[3:4])
		n++

		var indexConf IndexConfJSON
		err := readConfigFile(configFile, &indexConf)
		if err != nil {
			log.Fatal(err)
		}
		conf.IndexConfig.AttributeName = indexConf.AttributeName
		conf.IndexConfig.AttributeType = indexConf.AttributeType
		conf.IndexConfig.LBound = indexConf.LBound
		conf.IndexConfig.UBound = indexConf.UBound
		conf.IndexConfig.ConsLevel = indexConf.ConsLevel
		conf.IndexConfig.IndexStore.Store = indexConf.IndexStore.Store
		conf.IndexConfig.IndexStore.Endpoint = indexConf.IndexStore.Endpoint
		conf.IndexConfig.IndexStore.Bucket = indexConf.IndexStore.Bucket
		conf.IndexConfig.IndexStore.Implementation = indexConf.IndexStore.Implementation
	case "fault_injection":
		var function string
		var rate float64
		faultInjFlags := flag.NewFlagSet("fault injection QPU flags", flag.ExitOnError)
		faultInjFlags.StringVar(&function, "func", "noArg", "the type of fault to be injected")
		faultInjFlags.Float64Var(&rate, "rate", 0.0, "the fault rate")
		faultInjFlags.Parse(os.Args[3:5])
		n += 2

		conf.FaultInjectionConfig.Function = function
		conf.FaultInjectionConfig.Rate = float32(rate)
	}

	if conf.QpuType == "filter" || conf.QpuType == "cache" || conf.QpuType == "index" ||
		conf.QpuType == "federation" || conf.QpuType == "fault_injection" || conf.QpuType == "load_balancer" ||
		conf.QpuType == "lambda" {
		var connections string
		connFlags := flag.NewFlagSet("graph QPU flags", flag.ExitOnError)
		connFlags.StringVar(&connections, "conns", "noArg", "the QPU's outgoing connection to the graph")
		connFlags.Parse(os.Args[n:])

		conns := strings.Split(connections, "/")
		for _, c := range conns {
			conf.Connections = append(conf.Connections, c)
		}
	}

	err := server.Server(conf)
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
