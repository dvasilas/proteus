package config

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"runtime"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

//QPUConfig specifies the configuration structure of a QPU
type QPUConfig struct {
	QpuType     string
	Port        string
	Connections []struct {
		EndPoint string
		DataSet  struct {
			DB    string
			DC    string
			Shard string
		}
		DataStoreConfig DataStoreConfig
	}
	IndexConfig IndexConfig
}

//IndexConfig specifies the additional part of the configuration structure
//of an index QPU
type IndexConfig struct {
	Attribute  string
	IndexType  string
	LBound     string
	UBound     string
	ConsLevel  string
	IndexStore struct {
		Store  string
		Host   string
		Port   int
		Bucket string
	}
}

//DataStoreConfig specifies the additional parti of the configuration structure
//of a data store QPU
type DataStoreConfig struct {
	Type               string
	DataDir            string
	ΑwsAccessKeyID     string
	AwsSecretAccessKey string
	Endpoint           string
	BucketName         string
	LogStreamEndpoint  string
}

//---------------- API Functions -------------------

//GetConfig ...
func GetConfig(configFArg string, conf interface{}) error {
	viper.AutomaticEnv()

	err := viper.BindEnv("HOME")
	configF, err := getConfigFile(configFArg)
	if err != nil {
		return err
	}
	err = readConfigFile(configF, conf)
	if err != nil {
		return err
	}
	confJSON, err := json.Marshal(conf)
	if err != nil {
		return err
	}
	log.WithFields(log.Fields{
		"configuration": string(confJSON),
	}).Info("read configuration")
	return nil
}

//---------------- Auxiliary Functions -------------

func getConfigFile(arg string) (string, error) {
	var configF string
	if arg == "noArg" {
		confF := viper.Get("CONFIG_FILE")
		if confF == nil {
			return "", errors.New("QPU config file not specified")
		}
		configF = confF.(string)
	} else {
		configF = arg
	}
	return configF, nil
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
