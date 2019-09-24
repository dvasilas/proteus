package main

import (
	"context"
	"flag"
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	pb "github.com/dvasilas/proteus/src/protos/monitoring"
	"google.golang.org/grpc"
)

var (
	addr = flag.String("listen-address", ":2112", "The address to listen on for HTTP requests.")
)

var (
	rpcDurationsHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "index_qpu_query_response_time",
		Help:    "Query response time distributions.",
		Buckets: prometheus.ExponentialBuckets(0.1, 2, 32),
	})
)

func init() {
	prometheus.MustRegister(rpcDurationsHistogram)
}

func main() {
	conn, err := grpc.Dial("127.0.0.1:50050", grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	client := pb.NewMonitoringClient(conn)

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := client.LogResponseTimes(ctx)
	if err != nil {
		cancel()
		log.Fatal(err)
	}
	go func() {
		for {
			respT, err := stream.Recv()
			if err != nil {
				log.Fatal(err)
			}
			rpcDurationsHistogram.Observe(float64(respT.GetDuration()) / 1000000.0)
		}
	}()

	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(*addr, nil))
}
