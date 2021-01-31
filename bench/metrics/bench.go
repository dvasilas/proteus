package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"time"

	//	"encoding/binary"

	proteusclient "github.com/dvasilas/proteus/pkg/proteus-go-client"
)

var port = 50350

func main() {
	for {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), time.Duration(time.Second))
		if err != nil {
			time.Sleep(2 * time.Second)
			fmt.Println(fmt.Sprintf("retrying connecting to: 127.0.0.1:%d", port))
		} else {
			c.Close()
			break
		}
	}

	c, err := proteusclient.NewClient(proteusclient.Host{Name: "127.0.0.1", Port: port}, 256, 256, false)
	if err != nil {
		log.Fatal(err)
	}

	resp, err := c.GetMetrics()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(resp)

	fM, err := os.Create("measurements.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer fM.Close()

	if _, err := fmt.Fprintf(fM, "[freshnessLatency] p50(ms): %.5f\n", resp.FreshnessLatencyP50); err != nil {
		log.Fatal(err)
	}
	if _, err := fmt.Fprintf(fM, "[freshnessLatency] p90(ms): %.5f\n", resp.FreshnessLatencyP90); err != nil {
		log.Fatal(err)
	}
	if _, err := fmt.Fprintf(fM, "[freshnessLatency] p95(ms): %.5f\n", resp.FreshnessLatencyP95); err != nil {
		log.Fatal(err)
	}
	if _, err := fmt.Fprintf(fM, "[freshnessLatency] p99(ms): %.5f\n", resp.FreshnessLatencyP99); err != nil {
		log.Fatal(err)
	}
	if _, err := fmt.Fprintf(fM, "[FreshnessVersions] 0: %.5f\n", resp.FreshnessVersions0); err != nil {
		log.Fatal(err)
	}
	if _, err := fmt.Fprintf(fM, "[FreshnessVersions] 1: %.5f\n", resp.FreshnessVersions1); err != nil {
		log.Fatal(err)
	}
	if _, err := fmt.Fprintf(fM, "[FreshnessVersions] 2: %.5f\n", resp.FreshnessVersions2); err != nil {
		log.Fatal(err)
	}
	if _, err := fmt.Fprintf(fM, "[FreshnessVersions] 4: %.5f\n", resp.FreshnessVersions4); err != nil {
		log.Fatal(err)
	}
	if _, err := fmt.Fprintf(fM, "[DataTransfer] (kB): %.5f\n", resp.KBytesSent); err != nil {
		log.Fatal(err)
	}
}
