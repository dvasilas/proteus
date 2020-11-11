package datastoredriver

import (
	"context"
	"crypto/rand"
	"errors"
	"log"
	"math/big"
	"time"

	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/libqpu/utils"
	"github.com/dvasilas/proteus/internal/proto/qpu_api"
	mysqldriver "github.com/dvasilas/proteus/internal/qpu_classes/datastore_driver/mysql"
	s3driver "github.com/dvasilas/proteus/internal/qpu_classes/datastore_driver/s3"
	"github.com/dvasilas/proteus/pkg/proteus-go-client/pb"
	"github.com/golang/protobuf/ptypes"
	"github.com/opentracing/opentracing-go"
	"google.golang.org/grpc/benchmark/stats"
)

const stateDatabase = "stateDB"

var (
	histogramOpts = stats.HistogramOptions{
		// up to 2s
		NumBuckets:   200000,
		GrowthFactor: .01,
	}
)

// DsDriverQPU ...
type DsDriverQPU struct {
	state             libqpu.QPUState
	datastore         dataStore
	persistentQueries map[string]map[int]respChannels
	inputSchema       libqpu.Schema
	logTimestamps     bool
	histogram         *stats.Histogram
}

type respChannels struct {
	id      int
	logOpCh chan libqpu.LogOperation
	errCh   chan error
}

type dataStore interface {
	GetSnapshot(string, []string, []string, []string) (<-chan libqpu.LogOperation, <-chan error)
	SubscribeOps(string) (<-chan libqpu.LogOperation, context.CancelFunc, <-chan error)
}

// ---------------- API Functions -------------------

// InitClass ...
func InitClass(qpu *libqpu.QPU, catchUpDoneCh chan int) (*DsDriverQPU, error) {
	var ds dataStore
	var err error
	switch qpu.Config.DatastoreConfig.Type {
	case libqpu.MYSQL:
		ds, err = mysqldriver.NewDatastore(qpu.Config, qpu.InputSchema)
		if err != nil {
			return &DsDriverQPU{}, err
		}
	case libqpu.S3:
		ds, err = s3driver.NewDatastore(qpu.Config, qpu.InputSchema)
		if err != nil {
			return &DsDriverQPU{}, err
		}
	default:
		return &DsDriverQPU{}, utils.Error(errors.New("unknown datastore type"))
	}

	var histogram *stats.Histogram
	if qpu.Config.Evaluation.LogTimestamps {
		histogram = stats.NewHistogram(histogramOpts)
	}

	go func() {
		catchUpDoneCh <- 0
	}()

	return &DsDriverQPU{
		state:             qpu.State,
		datastore:         ds,
		persistentQueries: make(map[string]map[int]respChannels),
		inputSchema:       qpu.InputSchema,
		logTimestamps:     qpu.Config.Evaluation.LogTimestamps,
		histogram:         histogram,
	}, nil
}

// ProcessQuerySnapshot ...
func (q *DsDriverQPU) ProcessQuerySnapshot(query libqpu.ASTQuery, md map[string]string, sync bool, parentSpan opentracing.Span) (<-chan libqpu.LogOperation, <-chan error) {
	isNull, isNotNull := query.GetPredicateContains()
	return q.datastore.GetSnapshot(query.GetTable(), query.GetProjection(), isNull, isNotNull)
}

// ProcessQuerySubscribe ...
func (q *DsDriverQPU) ProcessQuerySubscribe(query libqpu.ASTQuery, md map[string]string, sync bool) (int, <-chan libqpu.LogOperation, <-chan error) {
	logOpCh := make(chan libqpu.LogOperation)
	errCh := make(chan error)
	n, err := rand.Int(rand.Reader, big.NewInt(1000000))
	if err != nil {
		errCh <- err
		close(logOpCh)
		close(errCh)
	}
	id := int(n.Int64())

	if _, found := q.persistentQueries[query.GetTable()]; !found {
		logOpChFromStore, cancel, errChFromStore := q.datastore.SubscribeOps(query.GetTable())
		q.persistentQueries[query.GetTable()] = make(map[int]respChannels, 1)

		q.persistentQueries[query.GetTable()][id] = respChannels{
			id:      id,
			logOpCh: logOpCh,
			errCh:   errCh,
		}
		go func() {
			for {
				select {
				case logOp, ok := <-logOpChFromStore:
					if !ok {
						logOpCh = nil
					} else {
						// utils.Trace("datastore received", map[string]interface{}{"logOp": logOp, "table": query.GetTable()})

						if q.logTimestamps {
							var t0, t1 time.Time
							t1 = time.Now()

							for _, v := range logOp.GetTimestamp().GetVc() {
								t0, err = ptypes.Timestamp(v)
								if err != nil {
									log.Fatal(err)
								}
							}

							q.histogram.Add(t1.Sub(t0).Nanoseconds())
							if err != nil {
								panic(utils.Error(err))
							}
						}

						if len(q.persistentQueries[query.GetTable()]) == 0 {
							cancel()
							delete(q.persistentQueries, query.GetTable())
							return
						}
						for _, respChs := range q.persistentQueries[query.GetTable()] {
							respChs.logOpCh <- logOp
						}
					}
				case err, ok := <-errChFromStore:
					if !ok {
						errCh = nil
					} else {
						if len(q.persistentQueries[query.GetTable()]) == 0 {
							cancel()
							delete(q.persistentQueries, query.GetTable())
							return
						}
						for _, respChs := range q.persistentQueries[query.GetTable()] {
							respChs.errCh <- err
							close(respChs.logOpCh)
							close(respChs.errCh)
						}
						return
					}
				}
				if logOpCh == nil && errCh == nil {
					for _, respChs := range q.persistentQueries[query.GetTable()] {
						close(respChs.logOpCh)
						close(respChs.errCh)
					}
				}
			}
		}()
	} else {

		q.persistentQueries[query.GetTable()][id] = respChannels{
			id:      id,
			logOpCh: logOpCh,
			errCh:   errCh,
		}
	}

	return id, logOpCh, errCh
}

// ClientQuery ...
func (q *DsDriverQPU) ClientQuery(query libqpu.ASTQuery, parentSpan opentracing.Span) (*pb.QueryResp, error) {
	return nil, nil
}

// RemovePersistentQuery ...
func (q *DsDriverQPU) RemovePersistentQuery(table string, queryID int) {
	if _, found := q.persistentQueries[table][queryID]; found {
		close(q.persistentQueries[table][queryID].logOpCh)
		close(q.persistentQueries[table][queryID].errCh)
		delete(q.persistentQueries[table], queryID)
	}
}

// GetConfig ...
func (q *DsDriverQPU) GetConfig() *qpu_api.ConfigResponse {
	schemaTables := make([]string, len(q.inputSchema))
	i := 0
	for table := range q.inputSchema {
		schemaTables[i] = table
		i++
	}
	return &qpu_api.ConfigResponse{
		Schema: schemaTables,
	}
}

// GetMetrics ...
func (q *DsDriverQPU) GetMetrics(*pb.MetricsRequest) (*pb.MetricsResponse, error) {
	return &pb.MetricsResponse{
		FreshnessLatencyP50: durationToMillis(time.Duration(pepcentile(.5, q.histogram))),
		FreshnessLatencyP90: durationToMillis(time.Duration(pepcentile(.9, q.histogram))),
		FreshnessLatencyP95: durationToMillis(time.Duration(pepcentile(.95, q.histogram))),
		FreshnessLatencyP99: durationToMillis(time.Duration(pepcentile(.99, q.histogram))),
	}, nil
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
