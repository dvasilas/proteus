package qpugraph

import (
	"net"
	"time"

	"github.com/dvasilas/proteus/internal/apiclient"
	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/libqpu/utils"
	"github.com/dvasilas/proteus/internal/proto/qpu_api"
)

// This package is responsible for the QPU's outgoing communications with the
// QPU graph.
// It implements functionalities including:
// - connecting the QPU with its adjacent QPUs
// - send queries to adjacent QPUs.

// ConnectToGraph initializes the QPU's connections to its adjacent QPUs in
// the QPU graph.
// For each adjacent QPU specified in the QPU's configuration:
// - a new client is created (see apiclient)
// - and libqpu.AdjacentQPU struct is created and added to the libqpu.QPU
//   struct
// TODO: request external configuration and capabilities from adjacent QPUs and
// update local view
func ConnectToGraph(qpu *libqpu.QPU) error {
	adjQPUs := make([]*libqpu.AdjacentQPU, len(qpu.Config.Connections))
	for i, conn := range qpu.Config.Connections {

		for {
			c, err := net.DialTimeout("tcp", conn.Address, time.Second)
			if err != nil {
				time.Sleep(2 * time.Second)
				utils.Trace("retrying connecting to", map[string]interface{}{"conn": conn.Address})
			} else {
				c.Close()
				break
			}
		}

		qpuapiclient, err := apiclient.NewClient(conn.Address)
		if err != nil {
			return err
		}

		adjQPUConf, err := qpuapiclient.GetConfig()
		if err != nil {
			return err
		}

		adjQPUs[i] = &libqpu.AdjacentQPU{
			APIClient:    qpuapiclient,
			OutputSchema: adjQPUConf.Schema,
		}
	}
	qpu.AdjacentQPUs = adjQPUs

	return nil
}

// SendQuery receives a query
// and an adjacent QPU (libqpu.AdjacentQPU struct), send a request with the
// given query to the given QPU, and returns a response stream handler
// (libqpu.ResponseStream struct) for receiving response records.
func SendQuery(query *qpu_api.Query, to *libqpu.AdjacentQPU) (libqpu.ResponseStream, error) {
	return to.APIClient.Query(
		libqpu.NewQueryRequest(query, nil, false),
	)
}
