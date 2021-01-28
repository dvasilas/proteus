package main

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
//	"encoding/binary"
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	proteusclient "github.com/dvasilas/proteus/pkg/proteus-go-client"
	"google.golang.org/grpc/benchmark/stats"
)

var dbName = "cloudServer"
var collectionName = "ycsbbuck"

var load = true
var insertOne = false
var updateOne = false
var execTime = 40

var dbSize = 33000
var attributeCard = dbSize / 10

var (
	histogramOpts = stats.HistogramOptions{
		// up to 500ms
		NumBuckets:   50000,
		GrowthFactor: .01,
	}
)

func main() {
	rand.Seed(time.Now().UTC().UnixNano())

	t, err := strconv.ParseInt(os.Args[1], 10, 64)
	if err != nil {
		log.Fatal(err)
	}
	threadCnt := int(t)

	for {
		c, err := net.DialTimeout("tcp", "127.0.0.1:50450", time.Duration(time.Second))
		if err != nil {
			time.Sleep(2 * time.Second)
			fmt.Println("retrying connecting to: 127.0.0.1:50450")
		} else {
			c.Close()
			break
		}
	}

	c, err := proteusclient.NewClient(proteusclient.Host{Name: "127.0.0.1", Port: 50450}, 256, 256, false)
	if err != nil {
		log.Fatal(err)
	}

	clientOptions := options.MergeClientOptions(
		&options.ClientOptions{
			Auth: &options.Credential{
				Username: "root",
				Password: "example",
			}}).ApplyURI("mongodb://127.0.0.1:27017/?replicaSet=rs0")

	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}

	for {
		err = client.Ping(context.Background(), nil)
		if err != nil {
			fmt.Println(err)
			time.Sleep(1 * time.Second)
			fmt.Println("retrying connecting")
		} else {
			break
		}
	}

	database := client.Database(dbName)
	col := database.Collection(collectionName)

	var results []*map[string]interface{}
	cur, err := col.Find(context.Background(), bson.M{}, options.Find())
	if err != nil {
		log.Fatal(err)
	}
	if err = cur.All(context.Background(), &results); err != nil {
		log.Fatal(err)
	}

	dataItems := make([]string, len(results))
	for i, result := range results {
		dataItems[i] = (*result)["_id"].(string)
	}


	opCntCh := make(chan int64)
	runtimeCh := make(chan time.Duration)

	hist := stats.NewHistogram(histogramOpts)

	start := time.Now()
	end := start.Add(time.Duration(execTime) * time.Second)
	var wg sync.WaitGroup

	for i := 0; i < threadCnt; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			opCnt := int64(0)
			for time.Now().UnixNano() < end.UnixNano() {
				r := rand.Float32()

				if r < 0.01 {
					_, err := col.InsertOne(context.TODO(), map[string]interface{}{
						"_id":        strconv.Itoa(rand.Int()),
						"attribute0": int64(rand.Intn(attributeCard)),
				//		"attribute1": int64(rand.Intn(attributeCard)),
				//		"attribute2": int64(rand.Intn(attributeCard)),
				//		"attribute3": int64(rand.Intn(attributeCard)),
				//		"attribute4": int64(rand.Intn(attributeCard)),
					})
					if err != nil {
						log.Fatal(err)
					}
				} else if r < 0.05 {
					dataItemID := dataItems[rand.Intn(len(dataItems))]

					_, err = col.UpdateOne(context.Background(),
						bson.D{{"_id", dataItemID}},
						bson.M{"$set": bson.M{"attribute0": rand.Intn(attributeCard)}},
					)
					if err != nil {
						log.Fatal(err)
					}
				} else {
					t0 := time.Now()
					_, err := c.Query1(fmt.Sprintf("select * from ycsbbuck where attribute0 = %d", int64(rand.Intn(attributeCard))))
					if err != nil {
						log.Fatal(err)
					}

					//fmt.Println(len(resp.GetRespRecord()))
					 //for _, r := range resp.GetRespRecord() {
					//	fmt.Println(r.GetResponse()["id"], int64(binary.LittleEndian.Uint64(r.GetResponse()["attribute0"].GetValue())))
					//}

					hist.Add(time.Since(t0).Nanoseconds())
				}
				opCnt++
			}
			end = time.Now()
			opCntCh <- opCnt
			runtimeCh <- end.Sub(start)
		}()
	}

	var aggOpCnt int64
	var opCntThreadCnt int
	var aggRuntime time.Duration
	var runtimeThreadCnt int

	for {
		select {
		case opCnt := <-opCntCh:
			aggOpCnt += opCnt
			opCntThreadCnt++
		case runtime := <-runtimeCh:
			aggRuntime += runtime
			runtimeThreadCnt++
		}
		if opCntThreadCnt == threadCnt && runtimeThreadCnt == threadCnt {
			close(opCntCh)
			close(runtimeCh)
			break
		}
	}

	wg.Wait()

	fmt.Println("threads: ", threadCnt)
	fmt.Println("operations: ", aggOpCnt)
	fmt.Println("throughput: ", float64(aggOpCnt)/aggRuntime.Seconds()*float64(threadCnt))
	fmt.Println("responseTime p50: ", durationToMillis(time.Duration(pepcentile(.5, hist))))
	fmt.Println("responseTime p90: ", durationToMillis(time.Duration(pepcentile(.90, hist))))
	fmt.Println("responseTime p99: ", durationToMillis(time.Duration(pepcentile(.99, hist))))
}

func pepcentile(percentile float64, h *stats.Histogram) int64 {
	percentileCount := int64(float64(h.Count) * percentile)
	currentCount := int64(0)
	for _, bucket := range h.Buckets {
		if currentCount+bucket.Count >= percentileCount {
			lastBuckedFilled := float64(percentileCount-currentCount) / float64(bucket.Count)
			return int64((1.0-lastBuckedFilled)*bucket.LowBound + lastBuckedFilled*bucket.LowBound*(1.0+histogramOpts.GrowthFactor))
		}
		currentCount += bucket.Count
	}
	panic("should have found a bound")
}

func durationToMillis(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}
