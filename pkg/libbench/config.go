package libbench

import (
	"io/ioutil"

	"github.com/BurntSushi/toml"
)

type benchmarkConfig struct {
	Connection struct {
		Endpoint        string
		Database        string
		AccessKeyID     string
		SecretAccessKey string
	}
	Benchmark struct {
		doPreload      bool
		measuredSystem string
		OpCount        int
		Runtime        int
		DoWarmup       bool
		Warmup         int
		ThreadCount    int
	}

	Operations struct {
		Homepage struct {
			StoriesLimit int
		}
		WriteRatio    float64
		DownVoteRatio float64
	}
	Preload struct {
		RecordCount struct {
			Users    int
			Stories  int
			Comments int
		}
	}
}

func getConfig(configFile string) (benchmarkConfig, error) {
	config := benchmarkConfig{}
	err := readConfigFile(configFile, &config)

	return config, err
}

func readConfigFile(configFile string, conf *benchmarkConfig) error {
	configData, err := ioutil.ReadFile(configFile)
	if err != nil {
		return nil
	}
	toml.Unmarshal(configData, conf)
	return nil
}
