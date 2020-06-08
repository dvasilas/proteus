package libbench

import (
	"io/ioutil"

	"github.com/BurntSushi/toml"
)

type benchmarkConfig struct {
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
