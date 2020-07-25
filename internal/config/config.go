package config

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"

	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/libqpu/utils"
	"github.com/dvasilas/proteus/internal/proto/qpu_api"
	toml "github.com/pelletier/go-toml"
)

// This package is responsible for parsing the configuration provided
// by a TOML file and populating libqpu.QPUConfig struct

type inputQPUConfig struct {
	QpuType     string
	Port        string
	Connections []struct {
		Address string
		Local   string
	}
	Schema []struct {
		Table      string
		Attributes []struct {
			Key  string
			Type string
		}
	}
	StateBackend struct {
		Endpoint    string
		Credentials struct {
			AccessKeyID     string
			SecretAccessKey string
		}
	}
	DatastoreConfig struct {
		Type              string
		Endpoint          string
		LogStreamEndpoint string
		DBName            string
		Credentials       struct {
			AccessKeyID     string
			SecretAccessKey string
		}
	}
	SumConfig struct {
		SourceTable       string
		RecordIDAttribute []string
		AttributeToSum    string
		Query             struct {
			Projection []string
			IsNull     []string
			IsNotNull  []string
		}
	}
	JoinConfig struct {
		Source []struct {
			Table      string
			Projection []string
		}
	}
	IndexConfig struct {
		Bucket        string
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
	CacheConfig struct {
		Size int
	}
	NetworkQPUConfig struct {
		Function string
		Rate     float32
		Delay    int64
	}
	Evaluation struct {
		Tracing       bool
		LogTimestamps bool
	}
	MaxWorkers  int
	MaxJobQueue int
}

// ---------------- API Functions -------------------

// GetQPUConfig parses the configuration file in the path indicated by
// 'configFile' and populates 'qpuConfig' accordingly
func GetQPUConfig(configFile string, qpu *libqpu.QPU) error {
	inputConfig := inputQPUConfig{}
	err := readConfigFile(configFile, &inputConfig)
	if err != nil {
		return err
	}

	config := &libqpu.QPUConfig{}

	// QpuType
	if err := getQpuType(inputConfig, config); err != nil {
		return err
	}

	// Port
	config.Port = inputConfig.Port

	// Connections
	if err := getConnections(inputConfig, config); err != nil {
		return err
	}

	// Schema
	if err := getSchema(inputConfig, qpu); err != nil {
		return err
	}

	// StateBackend
	if err := getStateConfig(inputConfig, config); err != nil {
		return err
	}

	// DatastoreConfiguration
	switch config.QpuType {
	case qpu_api.ConfigResponse_DATASTORE_DRIVER:
		if err := getDatastoreConfig(inputConfig, config); err != nil {
			return err
		}
	case qpu_api.ConfigResponse_SUM:
		if err := getSumConfig(inputConfig, config); err != nil {
			return err
		}
	case qpu_api.ConfigResponse_JOIN:
		if err := getJoinConfig(inputConfig, config); err != nil {
			return err
		}
	}

	// Evaluation
	if err := getEvaluation(inputConfig, config); err != nil {
		return err
	}

	// Misc
	config.MaxWorkers = inputConfig.MaxWorkers
	if val, isSet := os.LookupEnv("MAX_WORKERS"); isSet {
		maxWorkers, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return nil
		}
		config.MaxWorkers = int(maxWorkers)
	}
	config.MaxJobQueue = inputConfig.MaxJobQueue
	if val, isSet := os.LookupEnv("MAX_JOB_QUEUE"); isSet {
		maxWorkers, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return nil
		}
		config.MaxWorkers = int(maxWorkers)
	}

	qpu.Config = config
	fmt.Printf("%+v\n", qpu.Config)
	// utils.Trace("configuration parsed", map[string]interface{}{"config": qpu.Config})

	return nil
}

// ---------------- Internal Functions --------------

func readConfigFile(configFile string, conf *inputQPUConfig) error {
	configData, err := ioutil.ReadFile(configFile)
	if err != nil {
		return nil
	}
	return toml.Unmarshal(configData, conf)
}

func getQpuType(inputConf inputQPUConfig, config *libqpu.QPUConfig) error {
	switch inputConf.QpuType {
	case "datastore_driver":
		config.QpuType = qpu_api.ConfigResponse_DATASTORE_DRIVER
	case "sum":
		config.QpuType = qpu_api.ConfigResponse_SUM
	case "join":
		config.QpuType = qpu_api.ConfigResponse_JOIN
	default:
		return utils.Error(errors.New("unknown QPU type"))
	}
	return nil
}

func getConnections(inputConf inputQPUConfig, config *libqpu.QPUConfig) error {
	connections := make([]libqpu.QPUConnection, 0)
	for _, conn := range inputConf.Connections {
		var isLocal bool
		switch conn.Local {
		case "local":
			isLocal = true
		case "remote":
			isLocal = false
		default:
			return utils.Error(errors.New("connection should be either 'local' or 'remote'"))
		}

		connections = append(connections, libqpu.QPUConnection{
			Address: conn.Address,
			Local:   isLocal,
		})
	}
	config.Connections = connections

	return nil
}

func getSchema(inputConf inputQPUConfig, qpu *libqpu.QPU) error {
	qpu.Schema = make(map[string]map[string]libqpu.DatastoreAttributeType)
	for _, table := range inputConf.Schema {
		qpu.Schema[table.Table] = make(map[string]libqpu.DatastoreAttributeType)
		for _, attribute := range table.Attributes {
			switch attribute.Type {
			case "int":
				qpu.Schema[table.Table][attribute.Key] = libqpu.INT
			case "string":
				qpu.Schema[table.Table][attribute.Key] = libqpu.STR
			case "float":
				qpu.Schema[table.Table][attribute.Key] = libqpu.FLT
			default:
				return utils.Error(fmt.Errorf("invalid attribute type %s in schema", attribute.Type))
			}
		}
	}
	return nil
}

func getStateConfig(inputConf inputQPUConfig, config *libqpu.QPUConfig) error {
	config.StateBackend.Endpoint = inputConf.StateBackend.Endpoint
	config.StateBackend.Credentials.AccessKeyID = inputConf.StateBackend.Credentials.AccessKeyID
	config.StateBackend.Credentials.SecretAccessKey = inputConf.StateBackend.Credentials.SecretAccessKey

	return nil
}

func getDatastoreConfig(inputConf inputQPUConfig, config *libqpu.QPUConfig) error {
	switch inputConf.DatastoreConfig.Type {
	case "mysql":
		config.DatastoreConfig.Type = libqpu.MYSQL
	default:
		return utils.Error(errors.New("unknown datastore type"))
	}
	config.DatastoreConfig.DBName = inputConf.DatastoreConfig.DBName
	config.DatastoreConfig.Endpoint = inputConf.DatastoreConfig.Endpoint
	config.DatastoreConfig.LogStreamEndpoint = inputConf.DatastoreConfig.LogStreamEndpoint
	config.DatastoreConfig.Credentials.AccessKeyID = inputConf.DatastoreConfig.Credentials.AccessKeyID
	config.DatastoreConfig.Credentials.SecretAccessKey = inputConf.DatastoreConfig.Credentials.SecretAccessKey

	return nil
}

func getSumConfig(inputConf inputQPUConfig, config *libqpu.QPUConfig) error {
	config.SumConfig.SourceTable = inputConf.SumConfig.SourceTable
	config.SumConfig.RecordIDAttribute = inputConf.SumConfig.RecordIDAttribute
	config.SumConfig.AttributeToSum = inputConf.SumConfig.AttributeToSum

	config.SumConfig.Query.Projection = inputConf.SumConfig.Query.Projection
	config.SumConfig.Query.IsNull = inputConf.SumConfig.Query.IsNull
	config.SumConfig.Query.IsNotNull = inputConf.SumConfig.Query.IsNotNull

	return nil
}

func getJoinConfig(inputConf inputQPUConfig, config *libqpu.QPUConfig) error {
	joinConfig := make([]struct {
		Table      string
		Projection []string
	}, len(inputConf.JoinConfig.Source))
	for i, src := range inputConf.JoinConfig.Source {
		joinConfig[i] = struct {
			Table      string
			Projection []string
		}{
			Table:      src.Table,
			Projection: src.Projection,
		}
	}

	config.JoinConfig.Source = joinConfig

	return nil
}

func getEvaluation(inputConf inputQPUConfig, config *libqpu.QPUConfig) error {
	config.Evaluation.Tracing = inputConf.Evaluation.Tracing
	config.Evaluation.LogTimestamps = inputConf.Evaluation.LogTimestamps

	return nil
}
