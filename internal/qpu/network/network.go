package network

import (
	"errors"
	"io"
	"math/rand"
	"sort"
	"time"

	"github.com/dvasilas/proteus/internal"
	"github.com/dvasilas/proteus/internal/config"
	"github.com/dvasilas/proteus/internal/proto"
	"github.com/dvasilas/proteus/internal/proto/qpu_api"
	log "github.com/sirupsen/logrus"
)

const pingCount = 100

// NQPU implements a network QPU
type NQPU struct {
	qpu    *utils.QPU
	config *config.Config
}

//---------------- API Functions -------------------

// QPU creates a network QPU
func QPU(conf *config.Config) (*NQPU, error) {
	rand.Seed(time.Now().UnixNano())
	q := &NQPU{
		qpu: &utils.QPU{
			Config: conf,
		},
	}
	if len(conf.Connections) > 1 {
		return nil, errors.New("network QPUs support a single connection")
	}
	if err := utils.ConnectToQPUGraph(q.qpu); err != nil {
		return nil, err
	}

	if q.qpu.Config.NetworkQPUConfig.Function == "ping" {
		stream, _, err := q.qpu.Conns[0].Client.Forward()
		if err != nil {
			return nil, err
		}
		rtt := make([]time.Duration, pingCount)
		seqID := int64(0)
		for i := 0; i < pingCount; i++ {
			if err := stream.Send(protoutils.RequestStreamPing(seqID)); err != nil {
				return q, err
			}
			start := time.Now()
			p, err := stream.Recv()
			if err == io.EOF {
				return nil, err
			}
			rtt[i] = time.Since(start)
			if err != nil {
				return nil, err
			}
			seqID = p.GetSequenceId()
			log.WithFields(log.Fields{"ping": seqID, "RTT": rtt[i]}).Debug("network QPU")
			seqID++
		}
		sort.Sort(utils.ResponseTime(rtt))
		log.WithFields(log.Fields{"mean RTT": rtt[len(rtt)*50/100]}).Debug("network QPU")
		log.WithFields(log.Fields{"p90 RTT": rtt[len(rtt)*90/100]}).Debug("network QPU")
		log.WithFields(log.Fields{"p99 RTT": rtt[len(rtt)*99/100]}).Debug("network QPU")
	}

	return q, nil
}

// Query implements the Query API for the network QPU
func (q *NQPU) Query(streamOut qpu_api.QPU_QueryServer, query *qpu_api.QueryInternalQuery, metadata map[string]string, block bool) error {
	respRecordCh := make(chan *qpu_api.ResponseStreamRecord)
	canReturn := make(chan bool)
	go func(respRecordCh chan *qpu_api.ResponseStreamRecord) {
		for respRecord := range respRecordCh {
			if err := q.process(func() error {
				return streamOut.Send(respRecord)
			}); err != nil {
				log.WithFields(log.Fields{"error": err}).Debug("process.send")
			}
			if respRecord.GetType() == qpu_api.ResponseStreamRecord_END_OF_STREAM {
				canReturn <- true
				break
			}
		}
	}(respRecordCh)
	streamDown, _, err := q.qpu.Conns[0].Client.Forward()
	if err != nil {
		return err
	}
	if err = streamDown.Send(
		protoutils.RequestStreamRequest(
			&qpu_api.Query{
				Val: &qpu_api.Query_QueryI{
					QueryI: query,
				},
			},
			metadata,
			block,
		),
	); err != nil {
		return err
	}
	for {
		respRecord, err := streamDown.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		respRecordCh <- respRecord
		// if query.GetPing() != nil {
		// 	query, err = streamOut.Recv()
		// 	if err == io.EOF {
		// 		return err
		// 	}
		// 	if err != nil {
		// 		return err
		// 	}
		// 	if err := q.process(func() error {
		// 		return streamDown.Send(requestRecord)
		// 	}); err != nil {
		// 		return err
		// 	}
		// }
	}
	<-canReturn
	return nil
}

// GetConfig implements the GetConfig API for the network QPU
func (q *NQPU) GetConfig() (*qpu_api.ConfigResponse, error) {
	conf, err := q.qpu.Conns[0].Client.GetConfig()
	if err != nil {
		return nil, err
	}
	return conf, err
}

// GetDataTransfer ...
func (q *NQPU) GetDataTransfer() float32 {
	return 0
}

// Cleanup is called when the QPU receives a SIGTERM signcal
func (q *NQPU) Cleanup() {
	log.Info("network QPU cleanup")
}

//---------------- Internal Functions --------------

func (q *NQPU) process(send func() error) error {
	switch q.qpu.Config.NetworkQPUConfig.Function {
	case "drop":
		return q.drop(send)
	case "delay":
		return q.delay(send)
	}
	return nil
}

func (q *NQPU) drop(send func() error) error {
	if q.qpu.Config.NetworkQPUConfig.Rate < rand.Float32() {
		return send()
	}
	return nil
}

func (q *NQPU) delay(send func() error) error {
	time.Sleep(time.Millisecond * time.Duration(q.qpu.Config.NetworkQPUConfig.Delay))
	err := send()
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Debug("delay.send")
	}
	return err
}
