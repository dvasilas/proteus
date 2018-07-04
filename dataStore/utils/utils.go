package main

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	cli "github.com/dimitriosvasilas/modqp/dataStore/client"
)

const (
	address = "localhost:50051"
)

func random(min, max int64) int64 {
	return rand.Int63n(max-min) + min
}

func populateDS(c cli.Client) error {
	fmt.Println("Pre-populating data store ..")
	for i := 0; i < 100; i++ {
		key := "obj" + strconv.Itoa(i)
		attrs := map[string]int64{"size": random(0, 10*1024), "type": random(0, 10)}
		_, _, err := c.PutObjectMD(key, attrs)
		if err != nil {
			return err
		}
		time.Sleep(time.Millisecond)
	}
	return nil
}

func main() {
	rand.Seed(time.Now().Unix())
	c, conn := cli.NewClient(address)
	defer conn.Close()
	err := populateDS(c)
	if err != nil {
		log.Fatalf("failed: %v", err)
	}
}
