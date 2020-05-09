package lambda

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"sync"
	"time"

	"github.com/dvasilas/proteus/internal"
	"github.com/dvasilas/proteus/internal/config"
	"github.com/dvasilas/proteus/internal/proto"
	"github.com/dvasilas/proteus/internal/proto/qpu"
	"github.com/dvasilas/proteus/internal/proto/qpu_api"
	log "github.com/sirupsen/logrus"
)

// LQPU implements a load balancer QPU
type LQPU struct {
	qpu         *utils.QPU
	config      *config.Config
	cancelFuncs []context.CancelFunc
	state       lambdaState
}

type lambdaState interface {
	Initialize() error
	Update(*qpu.LogOperation) error
	Read() (interface{}, qpu.Vectorclock, error)
	GetTimestamp() (qpu.Vectorclock, error)
}

type lambdaCounter struct {
	counter int64
	clock   qpu.Vectorclock
	mutex   sync.RWMutex
}

func (l *lambdaCounter) Initialize() error {
	l.counter = 0
	l.clock = *(protoutils.Vectorclock(map[string]uint64{"": 0}))
	return nil
}

func (l *lambdaCounter) Update(logOp *qpu.LogOperation) error {
	log.WithFields(log.Fields{
		"operation": logOp,
	}).Debug("lambda update")

	l.mutex.Lock()
	l.counter++
	l.clock = *(logOp.GetTimestamp())
	l.mutex.Unlock()
	return nil
}

func (l *lambdaCounter) Read() (interface{}, qpu.Vectorclock, error) {
	l.mutex.RLock()
	c := l.counter
	ts, _ := l.GetTimestamp()
	l.mutex.RUnlock()
	return c, ts, nil
}

func (l *lambdaCounter) GetTimestamp() (qpu.Vectorclock, error) {
	return l.clock, nil
}

//---------------- API Functions -------------------

// QPU creates a fault injection QPU
func QPU(conf *config.Config) (*LQPU, error) {
	rand.Seed(time.Now().UnixNano())
	q := &LQPU{
		qpu: &utils.QPU{
			Config: conf,
		},
		state: &lambdaCounter{},
	}
	q.state.Initialize()

	if err := utils.ConnectToQPUGraph(q.qpu); err != nil {
		return nil, err
	}

	pred := []*qpu.AttributePredicate{}
	q.cancelFuncs = make([]context.CancelFunc, len(q.qpu.Conns))
	for i, conn := range q.qpu.Conns {
		streamIn, cancel, err := conn.Client.Query(
			//TODO: move this to configuration
			"lambda-buck",
			pred,
			protoutils.SnapshotTimePredicate(
				protoutils.SnapshotTime(qpu.SnapshotTime_INF, nil),
				protoutils.SnapshotTime(qpu.SnapshotTime_INF, nil),
			),
			nil,
			false,
		)
		if err != nil {
			cancel()
			return nil, err
		}
		q.cancelFuncs[i] = cancel
		go q.opConsumer(streamIn)
	}

	return q, nil
}

// Query implements the Query API for the fault injection QPU
func (q *LQPU) Query(streamOut qpu_api.QPU_QueryServer, query *qpu_api.QueryInternalQuery, metadata map[string]string, block bool) error {
	log.WithFields(log.Fields{"query": query, "QPU": "lambda"}).Debug("query received")
	_, err := streamOut.Recv()
	if err == io.EOF {
		return errors.New("Query received EOF")
	}
	if err != nil {
		return err
	}

	cnt, ts, _ := q.state.Read()
	if err := streamOut.Send(
		protoutils.ResponseStreamRecord(
			0,
			qpu_api.ResponseStreamRecord_STATE,
			protoutils.LogOperation(
				"lambda", "", qpu.LogOperation_S3OBJECT, &ts,
				protoutils.PayloadState(
					protoutils.ObjectState(
						[]*qpu.Attribute{protoutils.Attribute("counter", protoutils.ValueInt(cnt.(int64)))},
					),
				),
			),
		),
	); err != nil {
		return err
	}

	if err := streamOut.Send(
		protoutils.ResponseStreamRecord(
			1,
			qpu_api.ResponseStreamRecord_END_OF_STREAM,
			&qpu.LogOperation{},
		),
	); err != nil {
		return err
	}

	return nil
}

// GetConfig implements the GetConfig API for the fault injection QPU
func (q *LQPU) GetConfig() (*qpu_api.ConfigResponse, error) {
	resp := protoutils.ConfigRespοnse(
		q.qpu.Config.QpuType,
		q.qpu.QueryingCapabilities,
		q.qpu.Dataset)
	return resp, nil
}

// GetDataTransfer ...
func (q *LQPU) GetDataTransfer() float32 {
	return 0
}

// Cleanup is called when the QPU receives a SIGTERM signcal
func (q *LQPU) Cleanup() {
	log.Info("lambda QPU cleanup")
}

//----------- Stream Consumer Functions ------------

func (q *LQPU) opConsumer(stream qpu_api.QPU_QueryClient) {
	for {
		streamRec, err := stream.Recv()
		if err == io.EOF {
			// TODO: see datastoredriver to fix this
			log.Fatal("lambdaQPU:opConsumer received EOF, which is not expected")
			return
		} else if err != nil {
			log.Fatal("opConsumer err", err)
			return
		} else {
			if streamRec.GetType() == qpu_api.ResponseStreamRecord_UPDATEDELTA {
				log.WithFields(log.Fields{
					"operation": streamRec,
				}).Debug("lambda QPU received operation")

				q.state.Update(streamRec.GetLogOp())
			}
		}
	}
}

//---------------- Internal Functions --------------
