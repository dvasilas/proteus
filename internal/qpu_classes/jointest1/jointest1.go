package jointest1qpu

import (
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/metrics"
	"github.com/dvasilas/proteus/internal/proto/qpu_api"
	qpugraph "github.com/dvasilas/proteus/internal/qpuGraph"
	"github.com/dvasilas/proteus/internal/queries"
	responsestream "github.com/dvasilas/proteus/internal/responseStream"
	"github.com/dvasilas/proteus/pkg/proteus-go-client/pb"
	"github.com/opentracing/opentracing-go"
)

// TestQPU ...
type TestQPU struct {
	// state                      libqpu.QPUState
	// inputSchema                libqpu.Schema
	// outputSchema               libqpu.Schema
	subscribeQueries map[int]chan libqpu.LogOperation
	// aggregationAttribute       string
	// groupBy                    string
	schemaTable    string
	endOfStreamCnt int
	// inMemState                 *inMemState
	// port                       string
	catchUpDoneCh              chan int
	catchUpDone                bool
	measureNotificationLatency bool
	notificationLatencyM       metrics.LatencyM
}

// ---------------- API Functions -------------------

// InitClass ...
func InitClass(qpu *libqpu.QPU, catchUpDoneCh chan int) (*TestQPU, error) {
	rand.Seed(time.Now().UTC().UnixNano())

	fmt.Println("QPU type: join mock QPU 1")

	q := &TestQPU{
		// state:                      qpu.State,
		// inputSchema:                qpu.InputSchema,
		// outputSchema:               make(map[string]libqpu.SchemaTable),
		subscribeQueries: make(map[int]chan libqpu.LogOperation),
		// aggregationAttribute:       qpu.Config.AggregationConfig.AggregationAttribute,
		// groupBy:                    qpu.Config.AggregationConfig.GroupBy,
		// inMemState:                 &inMemState{entries: make(map[int64]*stateEntry)},
		// port:                       qpu.Config.Port,
		catchUpDoneCh:              catchUpDoneCh,
		measureNotificationLatency: qpu.Config.Evaluation.MeasureNotificationLatency,
		catchUpDone:                false,
	}

	if q.measureNotificationLatency {
		q.notificationLatencyM = metrics.NewLatencyM()
	}

	query := queries.NewQuerySnapshotAndSubscribe(
		"stories",
		[]string{"id", "user_id", "title", "description", "short_id"},
		[]string{},
		[]string{},
		nil,
	)

	responseStreamStories, err := qpugraph.SendQuery(libqpu.NewQuery(nil, query.Q), qpu.AdjacentQPUs[0])
	if err != nil {
		return nil, err
	}

	go func() {
		if err = responsestream.StreamConsumer(responseStreamStories, q.processRespRecord, nil, nil); err != nil {
			panic(err)
		}
	}()

	query = queries.NewQuerySnapshotAndSubscribe(
		"votes_sum",
		[]string{"story_id", "vote_sum"},
		[]string{},
		[]string{},
		nil,
	)
	responseStreamStories, err = qpugraph.SendQuery(libqpu.NewQuery(nil, query.Q), qpu.AdjacentQPUs[1])
	if err != nil {
		return nil, err
	}
	go func() {
		if err = responsestream.StreamConsumer(responseStreamStories, q.processRespRecord, nil, nil); err != nil {
			panic(err)
		}
	}()

	return q, nil
}

// ProcessQuerySnapshot ...
func (q *TestQPU) ProcessQuerySnapshot(query libqpu.ASTQuery, md map[string]string, sync bool, parentSpan opentracing.Span) (<-chan libqpu.LogOperation, <-chan error) {
	logOpCh := make(chan libqpu.LogOperation)
	errCh := make(chan error)

	return logOpCh, errCh
}

// ProcessQuerySubscribe ...
func (q *TestQPU) ProcessQuerySubscribe(query libqpu.ASTQuery, md map[string]string, sync bool) (int, <-chan libqpu.LogOperation, <-chan error) {
	logOpCh := make(chan libqpu.LogOperation)
	errCh := make(chan error)

	queryID := rand.Int()
	q.subscribeQueries[queryID] = logOpCh

	return queryID, logOpCh, errCh
}

// ClientQuery ...
func (q *TestQPU) ClientQuery(query libqpu.ASTQuery, parentSpan opentracing.Span) (*pb.QueryResp, error) {
	return nil, errors.New("not implemented")
}

// GetConfig ...
func (q *TestQPU) GetConfig() *qpu_api.ConfigResponse {
	return &qpu_api.ConfigResponse{}
}

// RemovePersistentQuery ...
func (q *TestQPU) RemovePersistentQuery(table string, queryID int) {
	delete(q.subscribeQueries, queryID)
}

// GetMetrics ...
func (q *TestQPU) GetMetrics(*pb.MetricsRequest) (*pb.MetricsResponse, error) {
	NL50, NL90, NL95, NL99 := q.notificationLatencyM.GetMetrics()

	return &pb.MetricsResponse{
		NotificationLatencyP50: NL50,
		NotificationLatencyP90: NL90,
		NotificationLatencyP95: NL95,
		NotificationLatencyP99: NL99,
	}, nil
}

// ---------------- Internal Functions --------------

func (q *TestQPU) processRespRecord(respRecord libqpu.ResponseRecord, data interface{}, recordCh chan libqpu.ResponseRecord) error {
	if q.catchUpDone && q.measureNotificationLatency {
		if err := q.notificationLatencyM.AddFromOp(respRecord.GetLogOp()); err != nil {
			return err
		}
	}

	respRecordType, err := respRecord.GetType()
	if err != nil {
		return err
	}

	if respRecordType == libqpu.EndOfStream {
		q.endOfStreamCnt++
		if q.endOfStreamCnt == 2 {
			q.catchUpDone = true

			go func() {
				q.catchUpDoneCh <- 0
			}()
		}
	} else if respRecordType == libqpu.State {

	}

	return nil
}
