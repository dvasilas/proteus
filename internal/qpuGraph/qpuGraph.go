package qpugraph

import (
	"net"
	"time"

	"github.com/dvasilas/proteus/internal/apiclient"
	"github.com/dvasilas/proteus/internal/libqpu"
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
			conn, _ := net.DialTimeout("tcp", conn.Address, time.Duration(time.Second))
			if conn != nil {
				conn.Close()
				break
			}
			time.Sleep(2 * time.Second)
		}

		qpuapiclient, err := apiclient.NewClient(conn.Address)
		if err != nil {
			return err
		}
		adjQPUs[i] = &libqpu.AdjacentQPU{
			APIClient: qpuapiclient,
		}
	}
	qpu.AdjacentQPUs = adjQPUs

	return nil
}

// SendQueryI receives a query (represented as a libqpu.InternalQuery struct)
// and an adjacent QPU (libqpu.AdjacentQPU struct), send a request with the
// given query to the given QPU, and returns a response stream handler
// (libqpu.ResponseStream struct) for receiving response records.
func SendQueryI(query libqpu.InternalQuery, to *libqpu.AdjacentQPU) (libqpu.ResponseStream, error) {
	return to.APIClient.Query(
		libqpu.NewQueryRequestI(query, nil, false),
	)
}

// SendQuerySQL has the same behavior as SendQueryI, but send an SQL string
// query.
func SendQuerySQL(query string, to *libqpu.AdjacentQPU) (libqpu.ResponseStream, error) {
	return to.APIClient.Query(
		libqpu.NewQueryRequestSQL(query, nil, false),
	)
}

// SendQueryUnary ...
func SendQueryUnary(query libqpu.InternalQuery, to *libqpu.AdjacentQPU) ([]libqpu.LogOperation, error) {
	resp, err := to.APIClient.QueryUnary(
		libqpu.NewQueryRequestI(query, nil, false),
	)

	response := make([]libqpu.LogOperation, len(resp.GetResults()))
	for i, entry := range resp.GetResults() {
		response[i] = libqpu.LogOperation{Op: entry}
	}
	return response, err
}
