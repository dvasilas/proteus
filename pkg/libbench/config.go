package libbench

import (
	"fmt"
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
		OpCount        int64
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
			Users      int
			Stories    int
			Comments   int
			StoryVotes int
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

func (c *benchmarkConfig) print() {
	fmt.Printf("Target system: %s\n", c.Benchmark.measuredSystem)
	fmt.Printf("Benchmark duration(s): %d\n", c.Benchmark.Runtime)
	fmt.Printf("Warmup(s): %d\n", c.Benchmark.Warmup)
	fmt.Printf("Benchmark threads: %d\n", c.Benchmark.ThreadCount)
	fmt.Printf("[workload] Q/W ratio(%%): %f\n", 1-c.Operations.WriteRatio)
	fmt.Printf("[workload] U/D vote ratio(%%): %f\n", 1-c.Operations.DownVoteRatio)
	fmt.Printf("[preload] Users: %d\n", c.Preload.RecordCount.Users)
	fmt.Printf("[preload] Stories: %d\n", c.Preload.RecordCount.Stories)
	fmt.Printf("[preload] Comments: %d\n", c.Preload.RecordCount.Comments)
	fmt.Printf("[preload] Votes-stories: %d\n", c.Preload.RecordCount.StoryVotes)
}
