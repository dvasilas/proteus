package main

import (
	"fmt"
	"log"
	"time"

	cli "modqp/data_store/client"
)

const (
	address = "localhost:50051"
)

func populateDS(c cli.Client) ([]int64, error) {
	fmt.Println("Pre-populating data store ..")
	keys := [5]string{"object001", "object002", "object003", "object004", "object005"}
	timestamps := make([]int64, 1000)
	for i, k := range keys {
		_, ts, err := c.PutObjectMD(k, "", "")
		if err != nil {
			return nil, err
		}
		timestamps[i] = ts
	}
	fmt.Println()
	return timestamps, nil
}

func main() {
	c, conn := cli.NewDSClient(address)
	defer conn.Close()
	timestamps, err := populateDS(c)
	if err != nil {
		log.Fatalf("failed: %v", err)
	}

	go c.Subscribe(timestamps[1])
	time.Sleep(time.Second)

	fmt.Println("Some write operations ..")
	_, ts, _ := c.PutObjectMD("object001", "size", "1024")
	time.Sleep(time.Second)
	timestamps[5] = ts
	_, ts, _ = c.PutObjectMD("object001", "size", "2048")
	timestamps[6] = ts
	time.Sleep(time.Second)
	_, ts, _ = c.PutObjectMD("object002", "size", "512")
	timestamps[7] = ts
	fmt.Println()
	time.Sleep(time.Second)

	/*
		fmt.Println("Reading object's latest state ..")
		resp, state, _ := c.GetObjectMD("object001", timestamps[6])
		fmt.Println(resp, state)
		fmt.Println()

		fmt.Println("Reading previous state ..")
		resp, state, _ = c.GetObjectMD("object001", timestamps[5])
		fmt.Println(resp, state)
		fmt.Println()

		fmt.Println("Getting snapshot..")
		c.GetSnapshot(timestamps[7])
		fmt.Println()

		c.GetSnapshot(timestamps[6])
		fmt.Println()

		fmt.Println("Getting operations..")
		c.GetOperations(timestamps[6])
		fmt.Println()
	*/
}
