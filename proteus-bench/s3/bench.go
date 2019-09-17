package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"sort"
	"time"

	"github.com/dvasilas/proteus/proteus-bench/s3/yelpDataset"
	"github.com/dvasilas/proteus/proteus_client"
	"github.com/dvasilas/proteus/src"
	pbS3Cli "github.com/dvasilas/proteus/src/protos/s3client"
	"google.golang.org/grpc"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/plotutil"
	"gonum.org/v1/plot/vg"
)

const bucket = "local-s3"
const preloadSize = 10
const workloadRation = 1.0
const workloadTime = 20

type dataset interface {
	PopulateDB(string, string, int, func(string, string, string, map[string]string) error) error
	Update(string, func(string, string, map[string]string) error) error
	Query() []proteusclient.AttributePredicate
}

type benchmark struct {
	workload      *yelp.Workload
	conf          config
	proteusClient *proteusclient.Client
	s3client      pbS3Cli.S3Client
	respTime      []time.Duration
}

type config struct {
	datasetSize     int
	maxAttributeNum int
	queryWriteRatio float32
	workloadMaxTime time.Duration
}

func (b *benchmark) populateDB(input string, doPopulate bool) error {
	err := b.createBucket(bucket)
	if err != nil {
		return err
	}
	if b.workload.PopulateDB(input, bucket, b.conf.datasetSize, doPopulate, b.putObject) != nil {
		return err
	}
	return nil
}

func (b *benchmark) doWorkload() (int64, float64, error) {
	t0 := time.Now()
	opCount := int64(0)
	for time.Since(t0) < b.conf.workloadMaxTime {
		r := rand.Float32()
		if r <= b.conf.queryWriteRatio {
			query := b.workload.Query()
			t, err := b.doQuery(query)
			if err != nil {
				return 0, 0, err
			}
			b.respTime = append(b.respTime, t)
			opCount++
		} else {
			if err := b.workload.Update(bucket, b.updateTags); err != nil {
				return 0, 0, err
			}
		}
	}
	t := time.Since(t0)
	return opCount, t.Seconds(), nil
}

func (b *benchmark) doQuery(query []proteusclient.AttributePredicate) (time.Duration, error) {
	t0 := time.Now()
	respCh, errCh, err := b.proteusClient.Query(query, proteusclient.LATESTSNAPSHOT)
	if err != nil {
		return time.Since(t0), err
	}
	eof := false
	responseStarted := false
	var elapsed time.Duration
	for !eof {
		select {
		case err := <-errCh:
			if err == io.EOF {
				eof = true
			} else {
				return time.Since(t0), err
			}
		case <-respCh:
			if !responseStarted {
				elapsed = time.Since(t0)
				responseStarted = true
			}
		}
	}
	return elapsed, nil
}

func (b *benchmark) plot() error {
	p, err := plot.New()
	if err != nil {
		return err
	}

	p.Title.Text = "CDF plot"
	p.X.Label.Text = "latency ms"
	p.Y.Label.Text = "cdf(x)"

	pts := make(plotter.XYs, len(b.respTime))
	for i, t := range b.respTime {
		pts[i].X = t.Seconds() * 1000
		pts[i].Y = float64(i) / float64(len(b.respTime)) * float64(100.0)
	}
	err = plotutil.AddLinePoints(p,
		"", pts,
	)
	if err != nil {
		return err
	}
	if err := p.Save(8*vg.Inch, 8*vg.Inch, "plot.pdf"); err != nil {
		return err
	}
	return nil
}

//---------------- S3 operations -------------------

func (b *benchmark) createBucket(bucketName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := b.s3client.CreateBucket(ctx, &pbS3Cli.CreateBucketRequest{BucketName: bucketName})
	return err
}

func (b *benchmark) putObject(bucketName, objName, content string, md map[string]string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := b.s3client.PutObject(ctx, &pbS3Cli.PutObjectRequest{
		ObjectName: objName,
		BucketName: bucketName,
		Content:    content,
		XAmzMeta:   md,
	})
	return err
}

func (b *benchmark) updateTags(bucketName, objName string, md map[string]string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := b.s3client.UpdateTags(ctx, &pbS3Cli.UpdateTagsRequest{
		ObjectName: objName,
		BucketName: bucketName,
		XAmzMeta:   md,
	})
	return err
}

//-------------------- main ------------------------

func main() {
	rand.Seed(time.Now().UnixNano())

	populate := flag.Bool("pp", false, "populate database")
	queryPort := flag.Int("port", 0, "qpu port")
	s3ClientEndP := flag.String("s3", "", "s3_client server port")
	dataset := flag.String("data", "", "dataset file")
	doPlot := flag.Bool("plot", false, "plot latency cdf")
	flag.Parse()

	if *queryPort < 50250 || *queryPort > 50550 {
		log.Fatal(errors.New("invalid query port"))
	}
	c, err := proteusclient.NewClient(proteusclient.Host{Name: "127.0.0.1", Port: *queryPort})
	if err != nil {
		log.Fatal(err)
	}
	conn, err := grpc.Dial("localhost:"+string(*s3ClientEndP), grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	b := benchmark{
		workload:      yelp.New(),
		proteusClient: c,
		s3client:      pbS3Cli.NewS3Client(conn),
		conf: config{
			datasetSize:     preloadSize,
			queryWriteRatio: workloadRation,
			workloadMaxTime: time.Second * workloadTime,
		},
		respTime: make([]time.Duration, 0),
	}

	if err := b.populateDB(*dataset, *populate); err != nil {
		log.Fatal(err)
	}
	opCount, duration, err := b.doWorkload()
	if err != nil {
		log.Fatal(err)
	}

	sort.Sort(utils.ResponseTime(b.respTime))
	l := len(b.respTime)
	fmt.Println("p50", b.respTime[l*50/100])
	fmt.Println("p90", b.respTime[l*90/100])
	fmt.Println("p95", b.respTime[l*95/100])
	fmt.Println("p99", b.respTime[l*99/100])

	fmt.Println("throughput:", float64(opCount)/duration)

	if *doPlot {
		if err := b.plot(); err != nil {
			log.Fatal(err)
		}
	}
}
