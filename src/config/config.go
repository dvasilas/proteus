package config

import (
	"errors"
	"strconv"

	"github.com/dvasilas/proteus/src/protos"
	pbQPU "github.com/dvasilas/proteus/src/protos/qpu"
	pbUtils "github.com/dvasilas/proteus/src/protos/utils"
	log "github.com/sirupsen/logrus"
)

// Config specifies the configuration structure of a QPU
type Config struct {
	QpuType         pbQPU.ConfigResponse_QPUType
	Port            string
	Connections     []QPUConnection
	DatastoreConfig struct {
		Dataset            *pbQPU.DataSet
		Type               Datastore
		Endpoint           string
		LogStreamEndpoint  string
		ΑwsAccessKeyID     string
		AwsSecretAccessKey string
	}
	CacheConfig struct {
		Size int
	}
	IndexConfig struct {
		IndexingConfig []*pbUtils.AttributePredicate
		Bucket         string
		ConsLevel      string
		IndexStore     struct {
			Store               IndexStore
			Endpoint            string
			Bucket              string
			IndexImplementation IndexImplementation
		}
	}
	NetworkQPUConfig struct {
		Function string
		Rate     float32
		Delay    int64
	}
}

// QPUConnection ...
type QPUConnection struct {
	Address string
	Local   bool
}

//---------------- API Functions -------------------

//GetConfig ...
func GetConfig(conf ConfJSON) (*Config, error) {
	log.WithFields(log.Fields{
		"configuration": conf,
	}).Info("read configuration")

	config := &Config{}
	if err := config.getQpuType(conf.QpuType); err != nil {
		return nil, err
	}
	config.Port = conf.Port
	connections := make([]QPUConnection, 0)
	for _, conn := range conf.Connections {
		var isLocal bool
		if conn.Local == "local" {
			isLocal = true
		} else if conn.Local == "remote" {
			isLocal = false
		} else {
			return nil, errors.New("connection should be either 'local' or 'remote'")
		}
		connections = append(connections, QPUConnection{
			Address: conn.Address,
			Local:   isLocal,
		})
	}
	config.Connections = connections

	switch config.QpuType {
	case pbQPU.ConfigResponse_DBDRIVER:
		if err := config.getDatastoreConfig(conf); err != nil {
			return nil, err
		}
	case pbQPU.ConfigResponse_CACHE:
		config.getCacheConfig(conf)
	case pbQPU.ConfigResponse_INDEX:
		if err := config.getIndexConfig(conf); err != nil {
			return nil, err
		}
	case pbQPU.ConfigResponse_NETWORK:
		config.getNetworkQPUConfig(conf)
	}
	return config, nil
}

//---------------- Internal Functions --------------

//QpuType ...
func (c *Config) getQpuType(t string) error {
	switch t {
	case "dbdriver":
		c.QpuType = pbQPU.ConfigResponse_DBDRIVER
	case "filter":
		c.QpuType = pbQPU.ConfigResponse_FILTER
	case "index":
		c.QpuType = pbQPU.ConfigResponse_INDEX
	case "cache":
		c.QpuType = pbQPU.ConfigResponse_CACHE
	case "federation":
		c.QpuType = pbQPU.ConfigResponse_FEDERATION_DISPATCHER
	case "load_balancer":
		c.QpuType = pbQPU.ConfigResponse_LOAD_BALANCER
	case "lambda":
		c.QpuType = pbQPU.ConfigResponse_LAMBDA
	case "network":
		c.QpuType = pbQPU.ConfigResponse_NETWORK
	case "intersection":
		c.QpuType = pbQPU.ConfigResponse_INTERSECTION

	default:
		return errors.New("unknown QPU type")
	}
	return nil
}

//-------- DatastoreDrive QPU Configuration --------

// Datastore ...
type Datastore int

const (
	// S3 is the enum value for an S3 backend data store
	S3 Datastore = iota
	// ANTIDOTE is the enum value for an Antidote backend data store
	ANTIDOTE Datastore = iota
	// MOCK ...
	MOCK Datastore = iota
)

func (c *Config) getDatastoreConfig(conf ConfJSON) error {
	c.DatastoreConfig.Endpoint = conf.DataStoreConfig.Endpoint
	c.DatastoreConfig.LogStreamEndpoint = conf.DataStoreConfig.LogStreamEndpoint
	c.DatastoreConfig.ΑwsAccessKeyID = conf.DataStoreConfig.AwsAccessKeyID
	c.DatastoreConfig.AwsSecretAccessKey = conf.DataStoreConfig.AwsSecretAccessKey
	c.DatastoreConfig.Dataset = protoutils.DataSet(
		map[string]map[string][]string{
			conf.DataStoreConfig.DataSet.DB: {
				conf.DataStoreConfig.DataSet.DC: {
					conf.DataStoreConfig.DataSet.Shard,
				},
			},
		})
	return c.getDatastore(conf.DataStoreConfig.Type)
}

func (c *Config) getDatastore(store string) error {
	switch store {
	case "s3":
		c.DatastoreConfig.Type = S3
	case "antidote":
		c.DatastoreConfig.Type = ANTIDOTE
	case "mock":
		c.DatastoreConfig.Type = MOCK
	default:
		return errors.New("unknown backend datastore")
	}
	return nil
}

//----------- Cache QPU Configuration --------------

func (c *Config) getCacheConfig(conf ConfJSON) {
	c.CacheConfig.Size = conf.CacheConfig.Size
}

// ----------- Network QPU Configuration ------------

func (c *Config) getNetworkQPUConfig(conf ConfJSON) {
	c.NetworkQPUConfig.Function = conf.NetworkQPUConfig.Function
	c.NetworkQPUConfig.Rate = conf.NetworkQPUConfig.Rate
	c.NetworkQPUConfig.Delay = conf.NetworkQPUConfig.Delay
}

//----------- Index QPU Configuration --------------

// IndexStore ...
type IndexStore int

const (
	// INMEM is the enum value for an in memory index
	INMEM IndexStore = iota
	// ANT is the enum value for an index stored in Antidote
	ANT IndexStore = iota
)

// IndexImplementation ...
type IndexImplementation int

func (c *Config) getIndexConfig(conf ConfJSON) error {
	c.IndexConfig.Bucket = conf.IndexConfig.Bucket
	if err := c.getIndexStore(conf); err != nil {
		return err
	}
	if err := c.getIndexImplementation(conf); err != nil {
		return err
	}
	if err := c.getIndexingConfig(conf); err != nil {
		return err
	}
	c.IndexConfig.IndexStore.Bucket = conf.IndexConfig.IndexStore.Bucket
	c.IndexConfig.ConsLevel = conf.IndexConfig.ConsLevel
	c.IndexConfig.IndexStore.Endpoint = conf.IndexConfig.IndexStore.Endpoint

	return nil
}

func (c *Config) getIndexStore(conf ConfJSON) error {
	switch conf.IndexConfig.IndexStore.Store {
	case "in_mem":
		c.IndexConfig.IndexStore.Store = INMEM
	case "antidote":
		c.IndexConfig.IndexStore.Store = ANT
	default:
		return errors.New("unknown index store type")
	}
	return nil
}

func (c *Config) getIndexImplementation(conf ConfJSON) error {
	return nil
}

// GetIndexConfig ...
func (c *Config) getIndexingConfig(conf ConfJSON) error {
	attrName, attrType, err := c.getIndexAttribute(conf)
	if err != nil {
		return err
	}
	lb, ub, err := c.getIndexBounds(attrType, conf)
	if err != nil {
		return err
	}
	c.IndexConfig.IndexingConfig = append(
		c.IndexConfig.IndexingConfig,
		protoutils.AttributePredicate(
			protoutils.Attribute(attrName, attrType, nil),
			lb, ub,
		),
	)
	return nil
}

// getIndexAttributeType ...
func (c *Config) getIndexAttribute(conf ConfJSON) (string, pbUtils.Attribute_AttributeType, error) {
	switch conf.IndexConfig.AttributeType {
	case "s3tagStr":
		return conf.IndexConfig.AttributeName, pbUtils.Attribute_S3TAGSTR, nil
	case "s3tagInt":
		return conf.IndexConfig.AttributeName, pbUtils.Attribute_S3TAGINT, nil
	case "s3tagFlt":
		return conf.IndexConfig.AttributeName, pbUtils.Attribute_S3TAGFLT, nil
	case "crdtCounter":
		return conf.IndexConfig.AttributeName, pbUtils.Attribute_CRDTCOUNTER, nil
	case "crdtLwwreg":
		return conf.IndexConfig.AttributeName, pbUtils.Attribute_CRDTLWWREG, nil
	default:
		return "", pbUtils.Attribute_S3TAGSTR, errors.New("unknown attribute type in index configuration")
	}
}

func (c *Config) getIndexBounds(t pbUtils.Attribute_AttributeType, conf ConfJSON) (*pbUtils.Value, *pbUtils.Value, error) {
	switch t {
	case pbUtils.Attribute_S3TAGSTR, pbUtils.Attribute_CRDTLWWREG:
		return protoutils.ValueStr(conf.IndexConfig.LBound), protoutils.ValueStr(conf.IndexConfig.UBound), nil
	case pbUtils.Attribute_S3TAGINT, pbUtils.Attribute_CRDTCOUNTER:
		lb, err := strconv.ParseInt(conf.IndexConfig.LBound, 10, 64)
		if err != nil {
			return nil, nil, err
		}
		ub, err := strconv.ParseInt(conf.IndexConfig.UBound, 10, 64)
		if err != nil {
			return nil, nil, err
		}
		return protoutils.ValueInt(lb), protoutils.ValueInt(ub), nil
	case pbUtils.Attribute_S3TAGFLT:
		lb, err := strconv.ParseFloat(conf.IndexConfig.LBound, 64)
		if err != nil {
			return nil, nil, err
		}
		ub, err := strconv.ParseFloat(conf.IndexConfig.UBound, 64)
		if err != nil {
			return nil, nil, err
		}
		return protoutils.ValueFlt(lb), protoutils.ValueFlt(ub), nil
	}
	return nil, nil, errors.New("index configuration: getIndexBounds not implemented for this attribute type")
}

//---------------- Auxiliary Functions -------------

// ConfJSON is used to read and marshal a JSON configuration file
type ConfJSON struct {
	QpuType         string
	Port            string
	Connections     []QPUConnectionJSON
	DataStoreConfig struct {
		DataSet struct {
			DB    string
			DC    string
			Shard string
		}
		Type               string
		Endpoint           string
		LogStreamEndpoint  string
		AwsAccessKeyID     string
		AwsSecretAccessKey string
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

// QPUConnectionJSON ...
type QPUConnectionJSON struct {
	Address string
	Local   string
}
