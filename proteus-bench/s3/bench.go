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
	pbS3Cli "github.com/dvasilas/proteus/src/protos/s3client"
	"google.golang.org/grpc"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/plotutil"
	"gonum.org/v1/plot/vg"
)

const bucket = "local-s3"
const preloadSize = 10
const workloadRation = 0.8
const workloadSize = 1000
const workloadTime = 10

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
	workloadMaxOPs  int
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

func (b *benchmark) doWorkload() error {
	t := time.Now()
	for i := 0; i < b.conf.workloadMaxOPs && time.Since(t) < b.conf.workloadMaxTime; i++ {
		r := rand.Float32()
		fmt.Println(r)
		if r <= b.conf.queryWriteRatio {
			query := b.workload.Query()
			fmt.Println(query)
			t, err := b.doQuery(query)
			if err != nil {
				return err
			}
			b.respTime = append(b.respTime, t)
		} else {
			if err := b.workload.Update(bucket, b.updateTags); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *benchmark) doQuery(query []proteusclient.AttributePredicate) (time.Duration, error) {
	start := time.Now()
	respCh, errCh, err := b.proteusClient.Query(query, proteusclient.LATESTSNAPSHOT)
	if err != nil {
		return time.Since(start), err
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
				return time.Since(start), err
			}
		case <-respCh:
			if !responseStarted {
				elapsed = time.Since(start)
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
	r, err := b.s3client.PutObject(ctx, &pbS3Cli.PutObjectRequest{
		ObjectName: objName,
		BucketName: bucketName,
		Content:    content,
		XAmzMeta:   md,
	})
	fmt.Println("r: ", r.GetReply() == 0)
	fmt.Println("err: ", err)
	return err
}

func (b *benchmark) updateTags(bucketName, objName string, md map[string]string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := b.s3client.UpdateTags(ctx, &pbS3Cli.UpdateTagsRequest{
		ObjectName: objName,
		BucketName: bucketName,
		XAmzMeta:   md,
	})
	fmt.Println("r: ", r.GetReply() == 0)
	fmt.Println("err: ", err)
	return err
}

//----------------- responseTime -------------------

type responseTime []time.Duration

func (t responseTime) Len() int {
	return len(t)
}
func (t responseTime) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}
func (t responseTime) Less(i, j int) bool {
	return t[i].Nanoseconds() < t[j].Nanoseconds()
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
			workloadMaxOPs:  workloadSize,
			workloadMaxTime: time.Second * workloadTime,
		},
		respTime: make([]time.Duration, 0),
	}

	if err := b.populateDB(*dataset, *populate); err != nil {
		log.Fatal(err)
	}
	if err := b.doWorkload(); err != nil {
		log.Fatal(err)
	}

	sort.Sort(responseTime(b.respTime))
	l := len(b.respTime)
	fmt.Println("p50", b.respTime[l*50/100])
	fmt.Println("p90", b.respTime[l*90/100])
	fmt.Println("p95", b.respTime[l*95/100])
	fmt.Println("p99", b.respTime[l*99/100])

	if *doPlot {
		if err := b.plot(); err != nil {
			log.Fatal(err)
		}
	}
}
