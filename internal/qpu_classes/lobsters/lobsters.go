package lobstersqpu

import (
	"errors"
	"fmt"
	"time"

	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/proto/qpuextapi"
	"github.com/opentracing/opentracing-go"

	"github.com/dvasilas/proteus/internal/proto/qpuapi"

	//
	_ "github.com/go-sql-driver/mysql"
)

const stateDatabase = "proteus_lobsters_db"

// LobQPU ...
type LobQPU struct {
	state libqpu.QPUState
	// inputSchema                libqpu.Schema
	// outputSchema               libqpu.Schema
	// stateTable                 string
	// joinAttributeKey           string
	// inMemState                 *inMemState
	// joinAttributes             map[string]string
	// endOfStreamCnt             int
	// catchUpDoneCh              chan int
	// port                       string
	// logTimestamps              bool
	// catchUpDone                bool
	// measureNotificationLatency bool
	// notificationLatencyM       metrics.LatencyM
	// stateUpdateM               metrics.LatencyM
	// writeLog                   writeLog
	// queryLog                   queryLog
}

// ---------------- API Functions -------------------

// InitClass ...
func InitClass(qpu *libqpu.QPU, catchUpDoneCh chan int) (*LobQPU, error) {

	fmt.Println("lobsters initclass")

	lqpu := &LobQPU{
		state: qpu.State,
		// inputSchema:                qpu.InputSchema,
		// outputSchema:               make(map[string]libqpu.SchemaTable),
		// joinAttributes:             qpu.Config.JoinConfig.JoinAttribute,
		// inMemState:                 &inMemState{entries: make(map[int64]*stateEntry)},
		// stateTable:                 qpu.Config.JoinConfig.OutputTableAlias,
		// joinAttributeKey:           qpu.Config.JoinConfig.JoinedAttributeAlias,
		// catchUpDoneCh:              catchUpDoneCh,
		// port:                       qpu.Config.Port,
		// logTimestamps:              qpu.Config.Evaluation.LogTimestamps,
		// measureNotificationLatency: qpu.Config.Evaluation.MeasureNotificationLatency,
		// catchUpDone:                false,
		// writeLog: writeLog{
		// 	entries: make([]libqpu.WriteLogEntry, 0),
		// },
		// queryLog: queryLog{
		// 	entries: make([]libqpu.QueryLogEntry, 0),
		// },
	}

	if err := lqpu.state.Init(stateDatabase, "", ""); err != nil {
		return nil, err
	}
	go func() {
		time.Sleep(time.Second * 2)
		catchUpDoneCh <- 0
	}()

	// for tableName, table := range jqpu.inputSchema {
	// 	for i := 0; i < len(qpu.AdjacentQPUs); i++ {
	// 		for _, t := range qpu.AdjacentQPUs[i].OutputSchema {
	// 			if t == tableName {
	// 				projection := make([]string, len(table.Attributes))
	// 				j := 0
	// 				for attr := range table.Attributes {
	// 					projection[j] = attr
	// 					j++
	// 				}
	// 				querySnapshot := queries.NewQuerySnapshotAndSubscribe(
	// 					tableName,
	// 					projection,
	// 					[]string{},
	// 					[]string{},
	// 					nil,
	// 				)
	// 				responseStreamStories, err := qpugraph.SendQuery(libqpu.NewQuery(nil, querySnapshot.Q), qpu.AdjacentQPUs[i])
	// 				if err != nil {
	// 					return nil, err
	// 				}
	// 				go func() {
	// 					if err = responsestream.StreamConsumer(responseStreamStories, qpu.Config.ProcessingConfig.Input.MaxWorkers, qpu.Config.ProcessingConfig.Input.MaxJobQueue, jqpu.processRespRecord, nil, nil); err != nil {
	// 						panic(err)
	// 					}
	// 				}()
	// 				break
	// 			}
	// 		}
	// 	}
	// }

	return lqpu, nil
}

// ProcessQuerySnapshot ...
func (q *LobQPU) ProcessQuerySnapshot(query libqpu.ASTQuery, md map[string]string, sync bool, parentSpan opentracing.Span) (<-chan libqpu.LogOperation, <-chan error) {

	logOpCh := make(chan libqpu.LogOperation)
	errCh := make(chan error)

	errCh <- errors.New("not implemented")

	return logOpCh, errCh
}

// ClientQuery ...
func (q *LobQPU) ClientQuery(query libqpu.ASTQuery, queryStr string, parentSpan opentracing.Span) (*qpuextapi.QueryResp, error) {

	return nil, nil
}

// ClientQuery1 ...
func (q *LobQPU) ClientQuery1(query libqpu.ASTQuery, queryStr string) (*qpuextapi.QueryResp1, error) {
	return nil, nil
}

// ProcessQuerySubscribe ...
func (q *LobQPU) ProcessQuerySubscribe(query libqpu.ASTQuery, md map[string]string, sync bool) (int, <-chan libqpu.LogOperation, <-chan error) {
	return -1, nil, nil
}

// QuerySubscribe  ...
func (q *LobQPU) QuerySubscribe(query libqpu.ASTQuery, res *qpuextapi.QueryReq) (chan libqpu.LogOperation, chan bool, chan error) {
	return nil, nil, nil
}

// RemovePersistentQuery ...
func (q *LobQPU) RemovePersistentQuery(table string, queryID int) {
}

// GetConfig ...
func (q *LobQPU) GetConfig() *qpuapi.ConfigResponse {
	return nil
}

// GetMetrics ...
func (q *LobQPU) GetMetrics(*qpuextapi.MetricsRequest) (*qpuextapi.MetricsResponse, error) {
	return nil, nil
}

// GetWriteLog ...
func (q *LobQPU) GetWriteLog(req *qpuextapi.GetWriteLogReq, stream qpuapi.QPUAPI_GetWriteLogServer) error {
	return nil
}

// ---------------- Internal Functions --------------
