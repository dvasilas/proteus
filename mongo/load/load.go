package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc/benchmark/stats"
)

var dbName = "cloudServer"
var collectionName = "ycsbbuck"

var load = true
var insertOne = false
var updateOne = false
var execTime = 40

var dbSize = 100
var attributeCnt = 10

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

	if load {
		_, err := col.DeleteMany(context.TODO(), bson.D{{}})
		if err != nil {
			log.Fatal(err)
		}

		entries := make([]interface{}, dbSize)
		for i := range entries {
			dataItem := map[string]interface{}{
				"_id":        fmt.Sprintf("dataitem%d", i),
				"attribute0": rand.Intn(attributeCard),
			}
			for j := 1; j <= rand.Intn(attributeCnt); j++ {
				dataItem[fmt.Sprintf("attribute%d", j)] = rand.Intn(attributeCard)
			}
			//			fmt.Println(dataItem)
			entries[i] = dataItem
		}

		_, err = col.InsertMany(context.TODO(), entries)
		if err != nil {
			log.Fatal(err)
		}
	}

	//	var results []*map[string]interface{}
	//	cur, err := col.Find(context.Background(), bson.M{}, options.Find())
	//	if err != nil {
	//		log.Fatal(err)
	//	}
	//	if err = cur.All(context.Background(), &results); err != nil {
	//		log.Fatal(err)
	//	}
	//
	//	dataItems := make([]string, len(results))
	//	for i, result := range results {
	//		dataItems[i] = (*result)["_id"].(string)
	//	}
	//
	//	fmt.Println(dataItems)

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
