package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/dvasilas/proteus/src/proto/monitoring"
	"google.golang.org/grpc"
)

var (
	addr = flag.String("listen-address", ":2112", "The address to listen on for HTTP requests.")
)

var (
	local_indexes_antidote_Histogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "local_indexes_antidote_QRT",
		Help:    "Query response time distributions.",
		Buckets: prometheus.ExponentialBuckets(0.1, 2, 32),
	})
)

var (
	local_indexes_inMem_Histogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "local_indexes_inmem_QRT",
		Help:    "Query response time distributions.",
		Buckets: prometheus.ExponentialBuckets(0.1, 2, 32),
	})
)

func init() {
	prometheus.MustRegister(local_indexes_antidote_Histogram)
	prometheus.MustRegister(local_indexes_inMem_Histogram)
}

func runWorkload(endpoint string) (monitoring.Monitoring_LogResponseTimesClient, context.CancelFunc, error) {
	conn, err := grpc.Dial(endpoint, grpc.WithInsecure())
	if err != nil {
		return nil, nil, err
	}
	client := monitoring.NewMonitoringClient(conn)

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := client.LogResponseTimes(ctx)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	return stream, cancel, err
}

func startWorkload(endpoint string) (monitoring.Monitoring_LogResponseTimesClient, error) {
	stream, cancel, err := runWorkload(endpoint)
	if err != nil {
		cancel()
		return nil, err
	}
	return stream, nil
}

func monitor(stream monitoring.Monitoring_LogResponseTimesClient, histogram prometheus.Histogram, endpoint string) {
	for {
		respT, err := stream.Recv()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(endpoint, ": ", respT.GetDuration())
		histogram.Observe(float64(respT.GetDuration()) / 1000000.0)
	}
}

func main() {
	stream1, err := startWorkload("127.0.0.1:50060")
	if err != nil {
		log.Fatal(err)
	}
	go monitor(stream1, local_indexes_antidote_Histogram, "127.0.0.1:50060")
	stream2, err := startWorkload("127.0.0.1:50061")
	if err != nil {
		log.Fatal(err)
	}
	go monitor(stream2, local_indexes_antidote_Histogram, "127.0.0.1:50061")
	stream3, err := startWorkload("127.0.0.1:50062")
	if err != nil {
		log.Fatal(err)
	}
	go monitor(stream3, local_indexes_antidote_Histogram, "127.0.0.1:50062")
	stream4, err := startWorkload("127.0.0.1:50063")
	if err != nil {
		log.Fatal(err)
	}
	go monitor(stream4, local_indexes_inMem_Histogram, "127.0.0.1:50063")

	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(*addr, nil))
}
