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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	proteusclient "github.com/dvasilas/proteus/pkg/proteus-go-client"
	"google.golang.org/grpc/benchmark/stats"
)

var execTime = 40
var attributeCard = 50

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

	awsRegion := "us-east-1"
	s3ForcePathStyle := true
	awsConf := aws.Config{
		Endpoint:         aws.String("http://127.0.0.1:8000"),
		Region:           &awsRegion,
		S3ForcePathStyle: &s3ForcePathStyle,
		Credentials:      credentials.NewStaticCredentials("accessKey1", "verySecretKey1", ""),
	}
	sess := session.Must(session.NewSession(&awsConf))
	svc := s3.New(sess)

	listObjectsIn := s3.ListObjectsV2Input{
		Bucket: aws.String("ycsbbuck"),
	}

	listObjectsRes, err := svc.ListObjectsV2(&listObjectsIn)
	if err != nil {
		log.Fatal(err)
	}

	bucket := make([]string, len(listObjectsRes.Contents))
	for i, object := range listObjectsRes.Contents {
		bucket[i] = *object.Key
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
					key := strconv.Itoa(rand.Int())
					putObjectIn := s3.PutObjectInput{
						Bucket: aws.String("ycsbbuck"),
						Key:    aws.String(key),
						Metadata: map[string]*string{
							"attribute0": aws.String(strconv.Itoa(rand.Intn(attributeCard))),
						},
					}
					_, err := svc.PutObject(&putObjectIn)
					if err != nil {
						log.Fatal(err)
					}

				} else if r < 0.05 {
					key := bucket[rand.Intn(len(bucket))]
					putObjectIn := s3.PutObjectInput{
						Bucket: aws.String("ycsbbuck"),
						Key:    aws.String(key),
						Metadata: map[string]*string{
							"attribute0": aws.String(strconv.Itoa(rand.Intn(attributeCard))),
						},
					}
					_, err := svc.PutObject(&putObjectIn)
					if err != nil {
						log.Fatal(err)
					}

				} else {

					t0 := time.Now()
					_, err := c.Query1(fmt.Sprintf("select * from ycsbbuck where attribute0 = %d", int64(rand.Intn(attributeCard))))
					if err != nil {
						log.Fatal(err)
					}

					// for _, r := range resp.GetRespRecord() {
					// 	fmt.Println(r.GetResponse()["id"], int64(binary.LittleEndian.Uint64(r.GetResponse()["attribute0"].GetValue())))
					// }

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
