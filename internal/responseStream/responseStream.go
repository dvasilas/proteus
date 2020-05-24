package responsestream

import (
	"io"

	"github.com/dvasilas/proteus/internal/libqpu"
)

// This package is responsible for implementing the functionality of receiving
// and processing records from a response stream, as a result of invoking the
// Query API of an adjacent QPU.

// StreamConsumer receives records from a libqpu.ResponseStream.
// For each record, it invokes the processLogOp function.
// When a record of type libqpu.EndOfStream is received, it closes the stream
// using the streams cancel function.
func StreamConsumer(stream libqpu.ResponseStream, processLogOp func(libqpu.ResponseRecord, interface{}, chan libqpu.ResponseRecord) error, data interface{}, recordCh chan libqpu.ResponseRecord) error {
	for {
		respRecord, err := stream.Recv()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
		respRecordType, err := respRecord.GetType()
		if err != nil {
			return err
		}
		if respRecordType == libqpu.EndOfStream {
			stream.Cancel()
			return nil
		}
		err = processLogOp(respRecord, data, recordCh)
		if err != nil {
			return err
		}
	}
}