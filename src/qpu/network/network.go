package network

import (
	"errors"
	"io"
	"math/rand"
	"sort"
	"time"

	"github.com/dvasilas/proteus/src"
	"github.com/dvasilas/proteus/src/config"
	"github.com/dvasilas/proteus/src/protos"
	pbQPU "github.com/dvasilas/proteus/src/protos/qpu"
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
func (q *NQPU) Query(streamUp pbQPU.QPU_QueryServer, requestRecUp *pbQPU.RequestStream) error {
	streamDown, _, err := q.qpu.Conns[0].Client.Forward()
	if err != nil {
		return err
	}
	if err = streamDown.Send(requestRecUp); err != nil {
		return err
	}
	for {
		requestRecDown, err := streamDown.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := q.process(func() error {
			return streamUp.Send(requestRecDown)
		}); err != nil {
			return err
		}
		if requestRecUp.GetPing() != nil {
			requestRecUp, err = streamUp.Recv()
			if err == io.EOF {
				return err
			}
			if err != nil {
				return err
			}
			if err := q.process(func() error {
				return streamDown.Send(requestRecUp)
			}); err != nil {
				return err
			}
		}
	}
}

// GetConfig implements the GetConfig API for the network QPU
func (q *NQPU) GetConfig() (*pbQPU.ConfigResponse, error) {
	resp := protoutils.ConfigRespοnse(
		q.qpu.Config.QpuType,
		q.qpu.QueryingCapabilities,
		q.qpu.Dataset)
	return resp, nil
}

// Cleanup is called when the QPU receives a SIGTERM signcal
func (q *NQPU) Cleanup() {
	log.Info("network QPU cleanup")
}

//---------------- Internal Functions --------------

func (q *NQPU) process(send func() error) error {
	switch q.qpu.Config.NetworkQPUConfig.Function {
	case "drop":
		return drop(q.qpu.Config.NetworkQPUConfig.Rate, send)
	case "delay":
		return delay(send)
	}
	return nil
}

func drop(rate float32, send func() error) error {
	if rate < rand.Float32() {
		log.WithFields(log.Fields{"action": "forward"}).Debug("network QPU: drop")
		return send()
	}
	log.WithFields(log.Fields{"action": "drop"}).Debug("network QPU: drop")
	return nil
}

func delay(send func() error) error {
	time.Sleep(time.Millisecond * 50)
	return send()
}
