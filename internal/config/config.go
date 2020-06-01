package config

import (
	"fmt"
	"io/ioutil"

	"github.com/dvasilas/proteus/internal/libqpu"
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
	if config.QpuType == qpu_api.ConfigResponse_DATASTORE_DRIVER {
		if err := getDatastoreConfig(inputConfig, config); err != nil {
			return err
		}
	} else if config.QpuType == qpu_api.ConfigResponse_SUM {
		if err := getSumConfig(inputConfig, config); err != nil {
			return err
		}
	}

	qpu.Config = config
	libqpu.Trace("configuration parsed", map[string]interface{}{"config": qpu.Config})

	return nil
}

// ---------------- Internal Functions --------------

func readConfigFile(configFile string, conf *inputQPUConfig) error {
	configData, err := ioutil.ReadFile(configFile)
	if err != nil {
		return nil
	}
	toml.Unmarshal(configData, conf)
	return nil
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
		return libqpu.Error("unknown QPU type")
	}
	return nil
}

func getConnections(inputConf inputQPUConfig, config *libqpu.QPUConfig) error {
	connections := make([]libqpu.QPUConnection, 0)
	for _, conn := range inputConf.Connections {
		var isLocal bool
		if conn.Local == "local" {
			isLocal = true
		} else if conn.Local == "remote" {
			isLocal = false
		} else {
			return libqpu.Error("connection should be either 'local' or 'remote'")
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
	qpu.Schema = make(map[string]map[string]libqpu.DatastoreAttributeType, 0)
	for _, table := range inputConf.Schema {
		qpu.Schema[table.Table] = make(map[string]libqpu.DatastoreAttributeType, 0)
		for _, attribute := range table.Attributes {
			switch attribute.Type {
			case "int":
				qpu.Schema[table.Table][attribute.Key] = libqpu.INT
			case "string":
				qpu.Schema[table.Table][attribute.Key] = libqpu.STR
			case "float":
				qpu.Schema[table.Table][attribute.Key] = libqpu.FLT
			default:
				return libqpu.Error(fmt.Sprintf("invalid attribute type %s in schema", attribute.Type))
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
		return libqpu.Error("unknown datastore type")
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
