package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc/benchmark/stats"
)

var load = false
var execTime = 20
var threadCnt = 1
var dbSize = 1000
var attributeCard = dbSize / 20

var (
	histogramOpts = stats.HistogramOptions{
		// up to 500ms
		NumBuckets:   50000,
		GrowthFactor: .01,
	}
)

func main() {
	rand.Seed(time.Now().UTC().UnixNano())

	clientOptions := options.MergeClientOptions(
		&options.ClientOptions{
			Auth: &options.Credential{
				Username: "root",
				Password: "example",
			}}).ApplyURI("mongodb://localhost:27017")

	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}

	for {
		err = client.Ping(context.Background(), nil)
		if err != nil {
			time.Sleep(2 * time.Second)
			fmt.Println("retrying connecting")
		} else {
			break
		}
	}

	database := client.Database("testCollection")

	col := database.Collection("testTable")

	if load {
		_, err = col.Indexes().CreateOne(
			context.Background(),
			mongo.IndexModel{
				Keys:    bson.M{"id": 1},
				Options: options.Index().SetUnique(true),
			},
		)
		if err != nil {
			log.Fatal(err)
		}

		_, err = col.Indexes().CreateOne(
			context.Background(),
			mongo.IndexModel{
				Keys: bson.M{"attribute0": 1},
			},
		)
		if err != nil {
			log.Fatal(err)
		}

		_, err = col.DeleteMany(context.TODO(), bson.D{{}})
		if err != nil {
			log.Fatal(err)
		}

		entries := make([]interface{}, dbSize)
		for i := range entries {
			entries[i] = map[string]interface{}{
				"id":         fmt.Sprintf("dataitem%d", i),
				"attribute0": int64(rand.Intn(attributeCard)),
			}
		}

		_, err = col.InsertMany(context.TODO(), entries)
		if err != nil {
			log.Fatal(err)
		}
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
				var results []*map[string]interface{}
				findOptions := options.Find()
				filter := bson.M{"attribute0": rand.Intn(attributeCard)}

				t0 := time.Now()
				cur, err := col.Find(context.Background(), filter, findOptions)
				if err != nil {
					log.Fatal(err)
				}
				if err = cur.All(context.Background(), &results); err != nil {
					log.Fatal(err)
				}

				hist.Add(time.Since(t0).Nanoseconds())

				opCnt++

				// for _, r := range results {
				// 	fmt.Println(r)
				// }
			}
			end = time.Now()
			fmt.Println("operations: ", opCnt)
			fmt.Println("runtime: ", end.Sub(start))
			fmt.Println("throughput: ", float64(opCnt)/end.Sub(start).Seconds())
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
			fmt.Println("got ", opCnt)
			aggOpCnt += opCnt
			opCntThreadCnt++
		case runtime := <-runtimeCh:
			fmt.Println("got ", runtime)
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
