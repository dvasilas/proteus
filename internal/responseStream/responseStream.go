package responsestream

import (
	"io"
	"time"

	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/libqpu/utils"
)

// This package is responsible for implementing the functionality of receiving
// and processing records from a response stream, as a result of invoking the
// Query API of an adjacent QPU.

// Job ...
type Job struct {
	respRecord   libqpu.ResponseRecord
	data         interface{}
	recordCh     chan libqpu.ResponseRecord
	processLogOp func(libqpu.ResponseRecord, interface{}, chan libqpu.ResponseRecord, int) error
	queryID      int
}

// Do ...
func (j *Job) Do() {
	err := j.processLogOp(j.respRecord, j.data, j.recordCh, j.queryID)
	if err != nil {
		utils.Error(err)
	}
}

// StreamConsumer receives records from a libqpu.ResponseStream.
// For each record, it invokes the processLogOp function.
// When a record of type libqpu.EndOfStream is received, it closes the stream
// using the streams cancel function.
func StreamConsumer(stream libqpu.ResponseStream, maxWorkers, maxQueue int, processLogOp func(libqpu.ResponseRecord, interface{}, chan libqpu.ResponseRecord, int) error, data interface{}, recordCh chan libqpu.ResponseRecord, queryID int) error {
	// dispatcher := workerpool.NewDispatcher(maxWorkers, maxQueue)
	// dispatcher.Run()

	for {
		respRecord, err := stream.Recv()
		// utils.Trace("StreamConsumer received", map[string]interface{}{"record": respRecord, "err": err})
		if err == io.EOF {
			return nil
		} else if err != nil {
			return utils.Error(err)
		}

		respRecord.InTs = time.Now()

		go func(respRecord libqpu.ResponseRecord, data interface{}, recordCh chan libqpu.ResponseRecord, queryID int) {
			// work := &Job{
			// 	respRecord:   respRecord,
			// 	data:         data,
			// 	recordCh:     recordCh,
			// 	processLogOp: processLogOp,
			// 	queryID:      queryID,
			// }
			err := processLogOp(respRecord, data, recordCh, queryID)
			if err != nil {
				utils.Error(err)
			}
			// dispatcher.JobQueue <- work
		}(respRecord, data, recordCh, queryID)
	}
}
