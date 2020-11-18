package responsestream

import (
	"io"
	"time"

	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/libqpu/utils"
	workerpool "github.com/dvasilas/proteus/internal/worker_pool"
)

// This package is responsible for implementing the functionality of receiving
// and processing records from a response stream, as a result of invoking the
// Query API of an adjacent QPU.

// Job ...
type Job struct {
	respRecord   libqpu.ResponseRecord
	data         interface{}
	recordCh     chan libqpu.ResponseRecord
	processLogOp func(libqpu.ResponseRecord, interface{}, chan libqpu.ResponseRecord) error
}

// Do ...
func (j *Job) Do() {
	err := j.processLogOp(j.respRecord, j.data, j.recordCh)
	if err != nil {
		utils.Error(err)
	}
}

// StreamConsumer receives records from a libqpu.ResponseStream.
// For each record, it invokes the processLogOp function.
// When a record of type libqpu.EndOfStream is received, it closes the stream
// using the streams cancel function.
func StreamConsumer(stream libqpu.ResponseStream, maxWorkers, maxQueue int, processLogOp func(libqpu.ResponseRecord, interface{}, chan libqpu.ResponseRecord) error, data interface{}, recordCh chan libqpu.ResponseRecord) error {
	dispatcher := workerpool.NewDispatcher(maxWorkers, maxQueue)
	dispatcher.Run()

	for {
		respRecord, err := stream.Recv()
		// utils.Trace("StreamConsumer received", map[string]interface{}{"record": respRecord, "err": err})
		if err == io.EOF {
			return nil
		} else if err != nil {
			return utils.Error(err)
		}

		respRecord.InTs = time.Now()

		work := &Job{
			respRecord:   respRecord,
			data:         data,
			recordCh:     recordCh,
			processLogOp: processLogOp,
		}

		dispatcher.JobQueue <- work
	}
}
