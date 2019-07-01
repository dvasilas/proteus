package main

import (
	"log"
	"net"
	"os"
	"testing"
	"time"

	"github.com/dvasilas/proteus/src/protos"
	"github.com/stretchr/testify/assert"

	"github.com/dvasilas/proteus/src/config"
	pbQPU "github.com/dvasilas/proteus/src/protos/qpu"
	pbUtils "github.com/dvasilas/proteus/src/protos/utils"
	"github.com/dvasilas/proteus/src/qpu/client"
	"github.com/dvasilas/proteus/src/qpu/datastore_driver"
	"github.com/dvasilas/proteus/src/qpu/filter"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type testDeployment struct {
	datastoreDriver *datastoredriver.DriverQPU
	filter          *filter.FQPU
	dsDriverConf    *config.Config
	filterConf      *config.Config
}

type testScenario struct {
	endpoint       string
	ops            []*pbUtils.LogOperation
	query          []*pbUtils.AttributePredicate
	assertObjectID string
}

var depl = testDeployment{
	dsDriverConf: &config.Config{
		QpuType: pbQPU.ConfigResponse_DBDRIVER,
		Port:    "50150",
	},
	filterConf: &config.Config{
		QpuType:     pbQPU.ConfigResponse_FILTER,
		Port:        "50250",
		Connections: []string{"localhost:50150"},
	},
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func deploy(conf *config.Config, api QPUAPI) <-chan int {
	wait := make(chan int)
	go func() {
		serverDS := QPUServer{config: conf, api: api}
		sDS := grpc.NewServer()
		pbQPU.RegisterQPUServer(sDS, &serverDS)
		reflection.Register(sDS)
		lis, err := net.Listen("tcp", ":"+conf.Port)
		if err != nil {
			log.Fatal(err)
		}
		wait <- 0
		sDS.Serve(lis)
	}()
	return wait
}

func doTest(t *testing.T, test testScenario) {
	client, err := client.NewClient(test.endpoint)
	if err != nil {
		t.Error(err)
	}

	for _, op := range test.ops {
		depl.datastoreDriver.Op(op)
	}

	stream, _, err := client.Query(
		test.query,
		protoutils.SnapshotTimePredicate(
			protoutils.SnapshotTime(pbUtils.SnapshotTime_ZERO, nil),
			protoutils.SnapshotTime(pbUtils.SnapshotTime_LATEST, nil),
		),
		false, false,
	)
	if err != nil {
		t.Error(err)
	}
	streamRec, err := stream.Recv()
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, test.assertObjectID, streamRec.GetLogOp().GetObjectId(), "")
}

func TestEtE_F(t *testing.T) {
	var err error

	depl.dsDriverConf.DatastoreConfig.Type = config.MOCK
	depl.datastoreDriver, err = datastoredriver.QPU(depl.dsDriverConf)
	if err != nil {
		log.Fatal(err)
	}
	wait := deploy(depl.dsDriverConf, depl.datastoreDriver)
	<-wait

	depl.filter, err = filter.QPU(depl.filterConf)
	if err != nil {
		log.Fatal(err)
	}

	wait = deploy(depl.filterConf, depl.filter)
	<-wait

	doTest(t, testF)

}

var testF = testScenario{
	endpoint: "localhost:50250",
	ops: []*pbUtils.LogOperation{
		protoutils.LogOperation(
			"object1",
			"mock_bucket",
			pbUtils.LogOperation_S3OBJECT,
			protoutils.Vectorclock(map[string]uint64{depl.dsDriverConf.Port: uint64(time.Now().Nanosecond())}),
			protoutils.PayloadDelta(nil,
				protoutils.ObjectState(
					[]*pbUtils.Attribute{
						protoutils.Attribute("attr", pbUtils.Attribute_S3TAGINT, protoutils.ValueInt(42)),
					},
				),
			),
		),
	},
	query: []*pbUtils.AttributePredicate{
		protoutils.AttributePredicate(protoutils.Attribute("attr", pbUtils.Attribute_S3TAGINT, nil),
			protoutils.ValueInt(40),
			protoutils.ValueInt(50),
		),
	},
	assertObjectID: "object1",
}
