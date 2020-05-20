package lambda

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"math/rand"
	"time"

	"github.com/dvasilas/proteus/internal"
	"github.com/dvasilas/proteus/internal/config"
	"github.com/dvasilas/proteus/internal/proto"
	"github.com/dvasilas/proteus/internal/proto/qpu"
	"github.com/dvasilas/proteus/internal/proto/qpu_api"
	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
)

// LQPU implements a load balancer QPU
type LQPU struct {
	qpu         *utils.QPU
	config      *config.Config
	cancelFuncs []context.CancelFunc
	state       *sql.DB
}

func (q *LQPU) update(logOp *qpu.LogOperation) error {
	log.WithFields(log.Fields{
		"operation": logOp,
	}).Debug("lambda update")

	stmtOut, err := q.state.Prepare("SELECT vote_count FROM stories_with_vote WHERE story_id = ?")
	if err != nil {
		panic(err.Error())
	}
	defer stmtOut.Close()

	var storyID, opVoteCount, stateVoteCount int64
	for _, attr := range logOp.GetPayload().GetDelta().GetNew().GetAttrs() {
		if attr.GetAttrKey() == "story_id" {
			storyID = attr.GetValue().GetInt()
		}
		if attr.GetAttrKey() == "vote" {
			opVoteCount = attr.GetValue().GetInt()
		}
	}
	err = stmtOut.QueryRow(storyID).Scan(&stateVoteCount)
	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			fmt.Println("no rows in result set")
			insForm, err := q.state.Prepare("INSERT INTO stories_with_vote (story_id, vote_count) VALUES(?,?)")
			if err != nil {
				panic(err.Error())
			}
			insForm.Exec(storyID, opVoteCount)
		} else {
			panic(err.Error())
		}
	} else {
		fmt.Printf("got vote_count: %d", stateVoteCount)
		stateVoteCount += opVoteCount
		insForm, err := q.state.Prepare("UPDATE stories_with_vote SET vote_count=? WHERE story_id=?")
		if err != nil {
			panic(err.Error())
		}
		insForm.Exec(stateVoteCount, storyID)

		fmt.Printf("new vote_count: %d", stateVoteCount)
	}

	return nil
}

//---------------- API Functions -------------------

// QPU creates a fault injection QPU
func QPU(conf *config.Config) (*LQPU, error) {
	rand.Seed(time.Now().UnixNano())
	q := &LQPU{
		qpu: &utils.QPU{
			Config: conf,
		},
	}

	if err := utils.ConnectToQPUGraph(q.qpu); err != nil {
		return nil, err
	}

	db, err := sql.Open("mysql", "root:123456@tcp(127.0.0.1:3308)/proteus")
	if err != nil {
		return nil, err
	}
	q.state = db

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
	q.state.Close()
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

				q.update(streamRec.GetLogOp())
			}
		}
	}
}

//---------------- Internal Functions --------------
