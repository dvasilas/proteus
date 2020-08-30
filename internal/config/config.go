package config

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"

	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/libqpu/utils"
	toml "github.com/pelletier/go-toml"
)

// This package is responsible for parsing the configuration provided
// by a TOML file and populating libqpu.QPUConfig struct

type inputQPUConfig struct {
	Operator    string
	State       string
	Port        string
	Connections []struct {
		Address string
		Local   string
	}
	InputSchema []struct {
		Table      string
		Attributes []struct {
			Key  string
			Type string
		}
		DownstreamQuery struct {
			IsNull    []string
			IsNotNull []string
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
	AggregationConfig struct {
		AggregationFunc      string
		AggregationAttribute string
		GroupBy              string
	}
	JoinConfig struct {
		Source []struct {
			Table         string
			JoinAttribute string
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

	// OperatorType
	if err := getOperatorType(inputConfig, config); err != nil {
		return err
	}

	// StateType
	if err := getStateType(inputConfig, config); err != nil {
		return err
	}

	// Port
	config.Port = inputConfig.Port

	// Connections
	if err := getConnections(inputConfig, config); err != nil {
		return err
	}

	// Schema
	if err := getInputSchema(inputConfig, qpu); err != nil {
		return err
	}

	// StateBackend
	if err := getStateConfig(inputConfig, config); err != nil {
		return err
	}

	// DatastoreConfiguration
	switch config.Operator {
	case libqpu.DBDriver:
		if err := getDatastoreConfig(inputConfig, config); err != nil {
			return err
		}
	case libqpu.Aggregation:
		if err := getAggregationConfig(inputConfig, config); err != nil {
			return err
		}
	case libqpu.Join:
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

func getOperatorType(inputConf inputQPUConfig, config *libqpu.QPUConfig) error {
	switch inputConf.Operator {
	case "db_driver":
		config.Operator = libqpu.DBDriver
	case "aggregation":
		config.Operator = libqpu.Aggregation
	case "join":
		config.Operator = libqpu.Join
	default:
		return utils.Error(errors.New("unknown operator type"))
	}
	return nil
}

func getStateType(inputConf inputQPUConfig, config *libqpu.QPUConfig) error {
	switch inputConf.State {
	case "stateless":
		config.State = libqpu.Stateless
	case "materialized_view":
		config.State = libqpu.MaterializedView
	default:
		return utils.Error(errors.New("unknown state type"))
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

func getInputSchema(inputConf inputQPUConfig, qpu *libqpu.QPU) error {
	qpu.InputSchema = make(libqpu.Schema)
	for _, table := range inputConf.InputSchema {
		qpu.InputSchema[table.Table] = libqpu.SchemaTable{
			Attributes: make(map[string]libqpu.DatastoreAttributeType),
			DownstreamQuery: libqpu.DownstreamQueryConf{
				IsNull:    table.DownstreamQuery.IsNull,
				IsNotNull: table.DownstreamQuery.IsNotNull,
			},
		}

		for _, attribute := range table.Attributes {
			switch attribute.Type {
			case "int":
				qpu.InputSchema[table.Table].Attributes[attribute.Key] = libqpu.INT
			case "string":
				qpu.InputSchema[table.Table].Attributes[attribute.Key] = libqpu.STR
			case "float":
				qpu.InputSchema[table.Table].Attributes[attribute.Key] = libqpu.FLT
			default:
				return utils.Error(fmt.Errorf("invalid attribute type %s in schema", attribute.Type))
			}
		}
	}
	fmt.Println("config/getInputSchema/qpu.InputSchema: ", qpu.InputSchema)
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

func getAggregationConfig(inputConf inputQPUConfig, config *libqpu.QPUConfig) error {
	switch inputConf.AggregationConfig.AggregationFunc {
	case "sum":
		config.AggregationConfig.AggregationFunc = libqpu.Sum
	default:
		return utils.Error(errors.New("unknown aggregation type"))
	}
	config.AggregationConfig.AggregationAttribute = inputConf.AggregationConfig.AggregationAttribute
	config.AggregationConfig.GroupBy = inputConf.AggregationConfig.GroupBy

	return nil
}

func getJoinConfig(inputConf inputQPUConfig, config *libqpu.QPUConfig) error {
	joinConfig := make(map[string]string)
	for _, src := range inputConf.JoinConfig.Source {
		joinConfig[src.Table] = src.JoinAttribute
	}

	config.JoinConfig.JoinAttribute = joinConfig

	return nil
}

func getEvaluation(inputConf inputQPUConfig, config *libqpu.QPUConfig) error {
	config.Evaluation.Tracing = inputConf.Evaluation.Tracing
	config.Evaluation.LogTimestamps = inputConf.Evaluation.LogTimestamps

	return nil
}
