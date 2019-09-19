package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dvasilas/proteus/proteus-bench/s3/yelpDataset"
	"github.com/dvasilas/proteus/proteus_client"
	"github.com/dvasilas/proteus/src"
	pbS3Cli "github.com/dvasilas/proteus/src/protos/s3client"
	"google.golang.org/grpc"

	log "github.com/sirupsen/logrus"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/plotutil"
	"gonum.org/v1/plot/vg"
)

const opCount = 10
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
	workload         *yelp.Workload
	conf             config
	proteusClient    *proteusclient.Client
	s3client         pbS3Cli.S3Client
	putObjectStream  pbS3Cli.S3_PutObjectClient
	updateTagsStream pbS3Cli.S3_UpdateTagsClient
	freshness        freshnessMeasurement
	responseTime     responseTimeMeasurement
}

type freshnessMeasurement struct {
	objectPutTs        map[int64]time.Time
	notificationTs     map[int64]time.Time
	freshnessIntervals []time.Duration
}

type responseTimeMeasurement struct {
	respTime []time.Duration
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

func (b *benchmark) runWorkload() (int64, float64, error) {
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
			b.responseTime.respTime = append(b.responseTime.respTime, t)
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

func (b *benchmark) subscribe(query []proteusclient.AttributePredicate, errorCh chan error) {
	respCh, errCh, err := b.proteusClient.Query(query, proteusclient.NOTIFY)
	if err != nil {
		log.Fatal(err)
	}

	eof := false
	for !eof {
		select {
		case err := <-errCh:
			if err == io.EOF {
				eof = true
			} else {
				log.Fatal(err)
			}
		case resp := <-respCh:
			t1 := time.Now()
			id, err := strconv.ParseInt(strings.TrimPrefix(resp.ObjectID, "obj"), 10, 64)
			if err != nil {
				log.Fatal(err)
			}
			b.freshness.notificationTs[id] = t1
			log.WithFields(log.Fields{
				"object":       resp.ObjectID,
				"objectPut ts": b.freshness.notificationTs[id],
			}).Info("notification")
			if id == opCount-1 {
				errorCh <- nil
				eof = true
			}
		}
	}
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

	pts := make(plotter.XYs, len(b.responseTime.respTime))
	for i, t := range b.responseTime.respTime {
		pts[i].X = t.Seconds() * 1000
		pts[i].Y = float64(i) / float64(len(b.responseTime.respTime)) * float64(100.0)
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

//------------------ freshness ---------------------

func (b *benchmark) howFresh() error {
	errCh := make(chan error)
	go b.subscribe(
		[]proteusclient.AttributePredicate{
			proteusclient.AttributePredicate{
				AttrName: "votes_useful",
				AttrType: proteusclient.S3TAGINT,
				Lbound:   int64(0),
				Ubound:   int64(10),
			},
		}, errCh)
	var putAvgT time.Duration
	time.Sleep(time.Second * 2)
	j := int64(0)
	for i := int64(0); i < opCount; i++ {
		t0 := time.Now()
		err := b.putObject("local-s3", "obj"+strconv.FormatInt(j, 10), "-------", map[string]string{"i-votes_useful": "0"})
		if err != nil {
			log.Fatal(err)
		}
		b.freshness.objectPutTs[j] = time.Now()
		log.WithFields(log.Fields{
			"object":       "obj" + strconv.FormatInt(j, 10),
			"objectPut ts": b.freshness.objectPutTs[j]}).Info("objectPut")
		putAvgT += time.Since(t0)
		j++
		time.Sleep(time.Second)
	}
	<-errCh
	for i := int64(0); i < int64(opCount); i++ {
		fmt.Println(" // ", b.freshness.objectPutTs[i], " -- ", b.freshness.notificationTs[i], " //")
		b.freshness.freshnessIntervals[i] = b.freshness.notificationTs[i].Sub(b.freshness.objectPutTs[i])
	}
	fmt.Println(b.freshness.freshnessIntervals)
	fmt.Println("average put latency", putAvgT/opCount)

	percentiles(b.freshness.freshnessIntervals)
	return nil
}

//-------------------- main ------------------------

func initialize(queryPort int, s3ClientEndP string) (*benchmark, *grpc.ClientConn, context.CancelFunc, error) {
	c, err := proteusclient.NewClient(proteusclient.Host{Name: "127.0.0.1", Port: queryPort})
	if err != nil {
		return nil, nil, nil, err
	}
	conn, err := grpc.Dial("localhost:"+string(s3ClientEndP), grpc.WithInsecure())
	if err != nil {
		return nil, nil, nil, err
	}

	b := &benchmark{
		workload:      yelp.New(),
		proteusClient: c,
		s3client:      pbS3Cli.NewS3Client(conn),
		conf: config{
			datasetSize:     preloadSize,
			queryWriteRatio: workloadRation,
			workloadMaxTime: time.Second * workloadTime,
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	b.putObjectStream, err = b.s3client.PutObject(ctx)
	if err != nil {
		cancel()
		return nil, nil, nil, err
	}

	ctx, cancel = context.WithCancel(context.Background())
	b.updateTagsStream, err = b.s3client.UpdateTags(ctx)
	if err != nil {
		cancel()
		return nil, nil, nil, err
	}

	return b, conn, cancel, nil
}

func main() {
	rand.Seed(time.Now().UnixNano())

	queryPort := flag.Int("port", 0, "qpu port")
	s3ClientEndP := flag.String("s3", "", "s3_client server port")
	dataset := flag.String("data", "", "dataset file")
	populate := flag.Bool("pp", false, "populate database")
	doPlot := flag.Bool("plot", false, "plot latency cdf")
	benchType := flag.String("bench", "", "benchmark type")
	flag.Parse()

	b, conn, cancel, err := initialize(*queryPort, *s3ClientEndP)
	defer conn.Close()
	defer cancel()
	if err != nil {
		log.Fatal(err)
	}

	if *benchType == "freshness" {
		b.freshness = freshnessMeasurement{
			objectPutTs:        make(map[int64]time.Time),
			notificationTs:     make(map[int64]time.Time),
			freshnessIntervals: make([]time.Duration, opCount),
		}
		b.howFresh()
	} else if *benchType == "response" {
		b.responseTime = responseTimeMeasurement{
			respTime: make([]time.Duration, 0),
		}
		if err := b.populateDB(*dataset, *populate); err != nil {
			log.Fatal(err)
		}
		opCount, duration, err := b.runWorkload()
		if err != nil {
			log.Fatal(err)
		}
		percentiles(b.responseTime.respTime)
		fmt.Println("throughput:", float64(opCount)/duration)
		if *doPlot {
			if err := b.plot(); err != nil {
				log.Fatal(err)
			}
		}
	}
}

//---------------- S3 operations -------------------

func (b *benchmark) createBucket(bucketName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := b.s3client.CreateBucket(ctx, &pbS3Cli.CreateBucketRequest{BucketName: bucketName})
	return err
}

func (b *benchmark) putObject(bucketName, objName, content string, md map[string]string) error {
	if err := b.putObjectStream.Send(&pbS3Cli.PutObjectRequest{
		ObjectName: objName,
		BucketName: bucketName,
		Content:    content,
		XAmzMeta:   md,
	}); err != nil {
		return err
	}
	resp, err := b.putObjectStream.Recv()
	if err != nil {
		return err
	}
	if resp.GetReply() == 0 {
		return nil
	}
	return errors.New("putObject err: code= " + strconv.FormatInt(int64(resp.GetReply()), 10))
}

func (b *benchmark) updateTags(bucketName, objName string, md map[string]string) error {
	if err := b.updateTagsStream.Send(&pbS3Cli.UpdateTagsRequest{
		ObjectName: objName,
		BucketName: bucketName,
		XAmzMeta:   md,
	}); err != nil {
		return err
	}
	resp, err := b.updateTagsStream.Recv()
	if err != nil {
		return err
	}
	if resp.GetReply() == 0 {
		return nil
	}
	return errors.New("updateTags err: code= " + strconv.FormatInt(int64(resp.GetReply()), 10))
}

//----------------- statistics ---------------------

func percentiles(durationArray []time.Duration) {
	sort.Sort(utils.ResponseTime(durationArray))
	l := len(durationArray)
	log.Info("p50: ", durationArray[l*50/100])
	log.Info("p90: ", durationArray[l*90/100])
	log.Info("p95: ", durationArray[l*95/100])
	log.Info("p99: ", durationArray[l*99/100])
}
