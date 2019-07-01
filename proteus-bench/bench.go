package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os/exec"
	"sort"
	"strconv"
	"time"

	"github.com/dvasilas/proteus/proteus_client"
)

type bench struct {
	conf config
}

type config struct {
	DatasetSize     int
	MaxAttributeNum int
}

type attribute struct {
	attrName string
	xAmzKey  string
	attrType proteusclient.AttributeType
	value    string
}

type predicate struct {
	attribute attribute
	lbound    interface{}
	ubound    interface{}
}

type query []predicate

func main() {
	b := bench{
		conf: config{
			DatasetSize:     100,
			MaxAttributeNum: 10,
		},
	}
	rand.Seed(time.Now().UnixNano())
	populate := flag.Bool("p", false, "populate database")
	flag.Parse()

	rand.Shuffle(len(attributePool), func(i, j int) { attributePool[i], attributePool[j] = attributePool[j], attributePool[i] })

	if *populate {
		if err := b.populateDB("local-s3", "127.0.0.1:8000"); err != nil {
			fmt.Println(err)
			log.Fatal(err)
		}
	}

	c, err := proteusclient.NewClient(proteusclient.Host{Name: "localhost", Port: 50450})
	if err != nil {
		log.Fatal(err)
	}

	workload := b.createQueryWorkload()
	respTime := make([]time.Duration, len(workload))
	for i, q := range workload {
		respTime[i] = doQuery(c, q)
	}
	sort.Sort(responseTime(respTime))
	fmt.Println(respTime)
	fmt.Println("mean", (respTime[49]+respTime[50])/2)
	fmt.Println("p90", respTime[89])
	fmt.Println("p95", respTime[94])
	fmt.Println("p99", respTime[98])
}

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

func (b *bench) createQueryWorkload() []query {
	workload := make([]query, 100)
	for i := range workload {
		q := make([]predicate, 0)
		q = append(q, predicate{
			attribute: attributePool[rand.Intn(b.conf.MaxAttributeNum)],
		})
		switch q[0].attribute.attrType {
		case proteusclient.S3TAGFLT:
			q[0].lbound = rand.Float64()
			q[0].ubound = q[0].lbound.(float64) + rand.Float64()*(1-q[0].lbound.(float64))
		case proteusclient.S3TAGINT:
			q[0].lbound = int64(rand.Intn(100))
			q[0].ubound = q[0].lbound.(int64) + int64(rand.Intn((100 - int(q[0].lbound.(int64)))))
		}
		workload[i] = q
	}
	return workload
}

func (b *bench) createAttributes() []attribute {
	attrN := rand.Intn(b.conf.MaxAttributeNum-1) + 1
	attributes := attributePool[:attrN]
	for i, attr := range attributes {
		switch attr.attrType {
		case proteusclient.S3TAGFLT:
			attributes[i].value = fmt.Sprintf("%f", rand.Float64())
		case proteusclient.S3TAGINT:
			attributes[i].value = strconv.Itoa(rand.Intn(100))
		}
	}
	return attributes
}

func (b *bench) populateDB(bucketName, endpoint string) error {
	createBucket(bucketName, endpoint)
	for i := 0; i < b.conf.DatasetSize; i++ {
		mdAttrs := b.createAttributes()
		if err := putObjectAndMD(bucketName, "obj"+strconv.Itoa(i), mdAttrs, endpoint); err != nil {
			return err
		}
	}
	return nil
}

func createBucket(bucketName, endpoint string) error {
	out, err := exec.Command("s3cmd", "mb", "s3://"+bucketName, "--host="+endpoint).Output()
	fmt.Println(string(out))
	return err
}

func putObject(bucketName, objName, endpoint string) error {
	args := []string{"put", "./bench.go", "s3://" + bucketName + "/" + objName, "--host=" + endpoint}
	out, err := exec.Command("s3cmd", args...).Output()
	fmt.Println(string(out))
	return err
}

func putObjectMD(bucketName, objName string, xAmzMeta []attribute, endpoint string) error {
	args := []string{"modify", "s3://" + bucketName + "/" + objName, "--host=" + endpoint}
	for _, attr := range xAmzMeta {
		args = append(args, "--add-header=x-amz-meta-"+attr.xAmzKey+":"+attr.value)
	}
	out, err := exec.Command("s3cmd", args...).Output()
	fmt.Println(string(out))
	return err
}

func putObjectAndMD(bucketName string, objName string, xAmzMeta []attribute, endpoint string) error {
	args := []string{"put", "./bench.go", "s3://" + bucketName + "/" + objName, "--host=" + endpoint}
	for _, attr := range xAmzMeta {
		args = append(args, "--add-header=x-amz-meta-"+attr.xAmzKey+":"+attr.value)
	}
	out, err := exec.Command("s3cmd", args...).Output()
	fmt.Println(string(out))
	return err
}

func doQuery(c *proteusclient.Client, query query) time.Duration {
	start := time.Now()
	q := make([]proteusclient.AttributePredicate, 0)
	for _, pred := range query {
		q = append(q, proteusclient.AttributePredicate{
			AttrName: pred.attribute.attrName,
			AttrType: pred.attribute.attrType,
			Lbound:   pred.lbound,
			Ubound:   pred.ubound,
		})
	}
	respCh, errCh, err := c.Query(q, proteusclient.LATEST_SNAPSHOT)
	if err != nil {
		log.Fatal(err)
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
				log.Fatal(err)
			}
		case <-respCh:
			//fmt.Println(resp)
			if !responseStarted {
				elapsed = time.Since(start)
				responseStarted = true
			}
		}
	}
	return elapsed
}

var attributePool = []attribute{
	{
		"attr0",
		"f-attr0",
		proteusclient.S3TAGFLT,
		"",
	},
	{
		"attr1",
		"f-attr1",
		proteusclient.S3TAGFLT,
		"",
	},
	{
		"attr2",
		"f-attr2",
		proteusclient.S3TAGFLT,
		"",
	},
	{
		"attr3",
		"f-attr3",
		proteusclient.S3TAGFLT,
		"",
	},
	{
		"attr4",
		"f-attr4",
		proteusclient.S3TAGFLT,
		"",
	},
	{
		"attr5",
		"f-attr5",
		proteusclient.S3TAGFLT,
		"",
	},
	{
		"attr6",
		"f-attr6",
		proteusclient.S3TAGFLT,
		"",
	},
	{
		"attr7",
		"f-attr7",
		proteusclient.S3TAGFLT,
		"",
	},
	{
		"attr8",
		"f-attr8",
		proteusclient.S3TAGFLT,
		"",
	},
	{
		"attr9",
		"f-attr9",
		proteusclient.S3TAGFLT,
		"",
	},
	{
		"attr10",
		"i-attr10",
		proteusclient.S3TAGINT,
		"",
	},
	{
		"attr11",
		"i-attr11",
		proteusclient.S3TAGINT,
		"",
	},
	{
		"attr12",
		"i-attr12",
		proteusclient.S3TAGINT,
		"",
	},
	{
		"attr13",
		"i-attr13",
		proteusclient.S3TAGINT,
		"",
	},
	{
		"attr14",
		"i-attr14",
		proteusclient.S3TAGINT,
		"",
	},
	{
		"attr15",
		"i-attr15",
		proteusclient.S3TAGINT,
		"",
	},
	{
		"attr16",
		"i-attr16",
		proteusclient.S3TAGINT,
		"",
	},
	{
		"attr17",
		"i-attr17",
		proteusclient.S3TAGINT,
		"",
	},
	{
		"attr18",
		"i-attr18",
		proteusclient.S3TAGINT,
		"",
	},
	{
		"attr19",
		"i-attr19",
		proteusclient.S3TAGINT,
		"",
	},
}
