package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dvasilas/proteus/proteus-bench/s3/yelpDataset"
	"github.com/dvasilas/proteus/proteus_client"
	"github.com/dvasilas/proteus/src"
	pbMonitoring "github.com/dvasilas/proteus/src/protos/monitoring"
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
const preloadSize = 100
const workloadRation = 0.9
const workloadTime = 20

type monitoringServer struct {
	benchmark *benchmark
}

type dataset interface {
	PopulateDB(string, int64, int64, func(string, string, string, map[string]string) error) error
	Update(string, func(string, string, map[string]string) error) error
}

type benchmark struct {
	workload      *yelp.Workload
	conf          config
	proteusClient *proteusclient.Client
	datastores    map[string]datastore
	freshness     freshnessMeasurement
	responseTime  responseTimeMeasurement
}

type datastore struct {
	client           pbS3Cli.S3Client
	putObjectStream  pbS3Cli.S3_PutObjectClient
	updateTagsStream pbS3Cli.S3_UpdateTagsClient
	conn             *grpc.ClientConn
	putObjectCancel  context.CancelFunc
	updateTagsCancel context.CancelFunc
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
	datasetSize      int64
	queryWriteRatio  float32
	workloadDuration time.Duration
}

func (b *benchmark) populateDB() error {
	offset := int64(0)
	for _, ds := range b.datastores {
		err := ds.createBucket(bucket)
		if err != nil {
			return err
		}
		if b.workload.PopulateDB(bucket, offset, b.conf.datasetSize, ds.putObject) != nil {
			return err
		}
		offset += b.conf.datasetSize
	}
	return nil
}

func (b *benchmark) runWorkload(dsEndpoint string, respTimeCh chan time.Duration) (int64, float64, error) {
	var qType int
	if os.Getenv("QUERY_TYPE") == "RANGE" {
		qType = 1
	} else if os.Getenv("QUERY_TYPE") == "POINT" {
		qType = 2
	}
	opCount := int64(0)
	for {
		r := rand.Float32()
		if r <= b.conf.queryWriteRatio {
			t := time.Duration(0)
			var err error
			var query []proteusclient.AttributePredicate
			if qType == 1 {
				query = b.workload.QueryRange()
			} else if qType == 2 {
				query = b.workload.QueryPoint()
			} else {
				log.Fatal("unknown value for QUERY_TYPE variable")
			}
			t, err = b.doQuery(query)
			if err != nil {
				return 0, 0, err
			}
			b.responseTime.respTime = append(b.responseTime.respTime, t)
			opCount++
			respTimeCh <- t
		} else {
			ds := b.datastores[dsEndpoint]
			if err := b.workload.Update(bucket, ds.updateTags); err != nil {
				return 0, 0, err
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
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

		var dsEndpoint string
		for k := range b.datastores {
			dsEndpoint = k
		}
		ds := b.datastores[dsEndpoint]

		err := ds.putObject("local-s3", "obj"+strconv.FormatInt(j, 10), "-------", map[string]string{"i-votes_useful": "0"})
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

func initDatastores(dbEndpoints []string) (*benchmark, error) {
	b := &benchmark{
		workload: yelp.New(),
		conf: config{
			datasetSize:      preloadSize,
			queryWriteRatio:  workloadRation,
			workloadDuration: workloadTime,
		},
	}

	datastores := make(map[string]datastore)
	for _, endpoint := range dbEndpoints {
		conn, err := grpc.Dial(endpoint, grpc.WithInsecure())
		if err != nil {
			return nil, err
		}
		client := pbS3Cli.NewS3Client(conn)
		ctx, putObjectCancel := context.WithCancel(context.Background())
		putObjectStr, err := client.PutObject(ctx)
		if err != nil {
			putObjectCancel()
			return nil, err
		}
		ctx, updateTagsCancel := context.WithCancel(context.Background())
		updateTagsStr, err := client.UpdateTags(ctx)
		if err != nil {
			putObjectCancel()
			updateTagsCancel()
			return nil, err
		}
		datastores[endpoint] = datastore{
			client:           client,
			putObjectStream:  putObjectStr,
			updateTagsStream: updateTagsStr,
			conn:             conn,
			putObjectCancel:  putObjectCancel,
			updateTagsCancel: updateTagsCancel,
		}
	}
	b.datastores = datastores
	return b, nil
}

func (b *benchmark) initProteus(proteusEndP string) error {
	proteusEndpoint := strings.Split(proteusEndP, ":")
	proteusPort, err := strconv.ParseInt(proteusEndpoint[1], 10, 64)
	if err != nil {
		log.Fatal(err)
	}
	proteusCli, err := proteusclient.NewClient(proteusclient.Host{Name: proteusEndpoint[0], Port: int(proteusPort)})
	if err != nil {
		log.Fatal(err)
	}
	b.proteusClient = proteusCli
	return nil
}

func serve(b *benchmark, port string) {
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	pbMonitoring.RegisterMonitoringServer(grpcServer, &monitoringServer{benchmark: b})

	log.WithFields(log.Fields{"port": port}).Info("listening")
	err = grpcServer.Serve(lis)
	log.Fatalf("server failed : %v", err)
}

func (s *monitoringServer) LogResponseTimes(stream pbMonitoring.Monitoring_LogResponseTimesServer) error {
	respTimeCh := make(chan time.Duration)
	s.benchmark.responseTime = responseTimeMeasurement{
		respTime: make([]time.Duration, 0),
	}
	go func() {
		for respT := range respTimeCh {
			if err := stream.Send(&pbMonitoring.ResponseTime{Duration: int64(respT)}); err != nil {
				log.Fatal(err)
			}
		}
	}()
	var dsEndpoint string
	for k := range s.benchmark.datastores {
		dsEndpoint = k
	}

	opCount, duration, err := s.benchmark.runWorkload(dsEndpoint, respTimeCh)
	if err != nil {
		return err
	}

	percentiles(s.benchmark.responseTime.respTime)
	fmt.Println("throughput:", float64(opCount)/duration)
	// if *doPlot {
	// 	if err := b.plot(); err != nil {
	// 		log.Fatal(err)
	// 	}
	// }
	return nil
}

func main() {
	initDebug()
	rand.Seed(time.Now().UnixNano())

	proteusEndP := flag.String("proteus", "", "proteus endpoint")
	dbEndP := flag.String("s3", "", "s3_client server port")
	flag.Parse()

	dbEndpoints := strings.Split(*dbEndP, "/")
	b, err := initDatastores(dbEndpoints)
	if err != nil {
		log.Fatal(err)
	}

	if err := b.populateDB(); err != nil {
		log.Fatal(err)
	}
	b.loadDataset()
	if err := b.initProteus(*proteusEndP); err != nil {
		log.Fatal(err)
	}

	respTimeCh := make(chan time.Duration)
	b.responseTime = responseTimeMeasurement{
		respTime: make([]time.Duration, 0),
	}
	go func() {
		for respT := range respTimeCh {
			fmt.Println(respT)
		}
	}()
	var dsEndpoint string
	for k := range b.datastores {
		dsEndpoint = k
	}

	_, _, err = b.runWorkload(dsEndpoint, respTimeCh)
	if err != nil {
		log.Fatal(err)
	}
}

//---------------- S3 operations -------------------

func (ds *datastore) createBucket(bucketName string) error {
	log.WithFields(log.Fields{"bucket": bucketName}).Debug("createBucket")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := ds.client.CreateBucket(ctx, &pbS3Cli.CreateBucketRequest{BucketName: bucketName})
	if err != nil {
		return err
	}
	log.WithFields(log.Fields{"reply": resp.GetReply()}).Debug("createBucket response")
	if resp.GetReply() == 0 {
		return nil
	}
	return errors.New("createBucket err: code= " + strconv.FormatInt(int64(resp.GetReply()), 10))
}

func (ds *datastore) putObject(bucketName, objName, content string, md map[string]string) error {
	log.WithFields(log.Fields{"bucket": bucketName, "object": objName}).Debug("putObject")
	if err := ds.putObjectStream.Send(&pbS3Cli.Object{
		ObjectName: objName,
		BucketName: bucketName,
		Content:    content,
		XAmzMeta:   md,
	}); err != nil {
		return err
	}
	resp, err := ds.putObjectStream.Recv()
	if err != nil {
		return err
	}
	log.WithFields(log.Fields{"reply": resp.GetReply()}).Debug("putObject response")
	if resp.GetReply() == 0 {
		return nil
	}
	return errors.New("putObject err: code= " + strconv.FormatInt(int64(resp.GetReply()), 10))
}

func (ds *datastore) updateTags(bucketName, objName string, md map[string]string) error {
	if err := ds.updateTagsStream.Send(&pbS3Cli.UpdateTagsRequest{
		ObjectName: objName,
		BucketName: bucketName,
		XAmzMeta:   md,
	}); err != nil {
		return err
	}
	resp, err := ds.updateTagsStream.Recv()
	if err != nil {
		return err
	}
	if resp.GetReply() == 0 {
		return nil
	}
	return errors.New("updateTags err: code= " + strconv.FormatInt(int64(resp.GetReply()), 10))
}

func (b *benchmark) loadDataset() error {
	ctx, cancel := context.WithCancel(context.Background())

	var dsEndpoint string
	for k := range b.datastores {
		dsEndpoint = k
	}
	ds := b.datastores[dsEndpoint]

	stream, err := ds.client.LoadDataset(ctx, &pbS3Cli.LoadDatasetRequest{})
	if err != nil {
		cancel()
		return err
	}
	ch := b.workload.LoadDataset()
	for {
		obj, err := stream.Recv()
		if err != nil {
			cancel()
			break
		}
		ch <- yelp.Object{
			Key:    obj.GetObjectName(),
			Bucket: obj.GetBucketName(),
			Md:     obj.GetXAmzMeta(),
		}
	}
	b.workload.PrintDataset()
	return nil
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

func initDebug() error {
	debug := os.Getenv("DEBUG")
	if debug == "true" {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}
	return nil
}
