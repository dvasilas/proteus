package server

import (
	"log"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/dvasilas/proteus/src/protos"
	"github.com/stretchr/testify/assert"

	"github.com/dvasilas/proteus/src/config"
	pbQPU "github.com/dvasilas/proteus/src/protos/qpu"
	pbUtils "github.com/dvasilas/proteus/src/protos/utils"
	"github.com/dvasilas/proteus/src/qpu/cache"
	"github.com/dvasilas/proteus/src/qpu/client"
	"github.com/dvasilas/proteus/src/qpu/datastore_driver"
	"github.com/dvasilas/proteus/src/qpu/filter"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type deployment struct {
	config          []*config.Config
	datastoredriver *datastoredriver.DriverQPU
	servers         []*grpc.Server
}

type testScenario struct {
	deployment deployment
	endpoint   string
	plan       queryPlan
}

type queryPlan []struct {
	query          []*pbUtils.AttributePredicate
	assertResponse string
}

type datastoreDeployment struct {
	config          *config.Config
	datastoredriver *datastoredriver.DriverQPU
	ops             []*pbUtils.LogOperation
}

var singleDatastore = datastoreDeployment{
	config: &config.Config{
		QpuType: pbQPU.ConfigResponse_DBDRIVER,
		Port:    "50150",
	},
	ops: []*pbUtils.LogOperation{
		protoutils.LogOperation(
			"object1",
			"mock_bucket",
			pbUtils.LogOperation_S3OBJECT,
			protoutils.Vectorclock(map[string]uint64{"50150": uint64(time.Now().Nanosecond())}),
			protoutils.PayloadDelta(nil,
				protoutils.ObjectState(
					[]*pbUtils.Attribute{
						protoutils.Attribute("attr", pbUtils.Attribute_S3TAGINT, protoutils.ValueInt(42)),
					},
				),
			),
		),
	},
}

var tests = []testScenario{
	testScenario{
		deployment: deployment{
			config: []*config.Config{
				&config.Config{
					QpuType: pbQPU.ConfigResponse_FILTER,
					Port:    "50250",
					Connections: []config.QPUConnection{config.QPUConnection{
						Address: "127.0.0.1:50150",
						Local:   true,
					},
					},
				},
			},
		},
		endpoint: "127.0.0.1:50250",
		plan: queryPlan{
			{
				query: []*pbUtils.AttributePredicate{
					protoutils.AttributePredicate(protoutils.Attribute("attr", pbUtils.Attribute_S3TAGINT, nil),
						protoutils.ValueInt(40),
						protoutils.ValueInt(50),
					),
				},
				assertResponse: "object1",
			},
		},
	},
	{
		deployment: deployment{
			config: []*config.Config{
				&config.Config{
					QpuType: pbQPU.ConfigResponse_FILTER,
					Port:    "50250",
					Connections: []config.QPUConnection{config.QPUConnection{
						Address: "127.0.0.1:50150",
						Local:   true,
					},
					},
				},
				&config.Config{
					QpuType: pbQPU.ConfigResponse_CACHE,
					Port:    "50451",
					Connections: []config.QPUConnection{config.QPUConnection{
						Address: "127.0.0.1:50250",
						Local:   true,
					},
					},
				},
			},
		},
		endpoint: "127.0.0.1:50451",
		plan: queryPlan{
			{
				query: []*pbUtils.AttributePredicate{
					protoutils.AttributePredicate(protoutils.Attribute("attr", pbUtils.Attribute_S3TAGINT, nil),
						protoutils.ValueInt(40),
						protoutils.ValueInt(50),
					),
				},
				assertResponse: "object1",
			},
		},
	},
}

func TestEndToEnd(t *testing.T) {
	deployPopulateDatastore(singleDatastore)
	cmd := exec.Command("../../../deployment/wait-for-it.sh", "--host=127.0.0.1", "--port=50150", "--timeout=0")
	err := cmd.Run()
	if err != nil {
		log.Fatal(err)
	}

	for _, tt := range tests {
		deploy(&tt.deployment)
		doTest(t, tt)
		for _, s := range tt.deployment.servers {
			s.Stop()
		}
	}
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func deployPopulateDatastore(depl datastoreDeployment) {
	depl.config.DatastoreConfig.Type = config.MOCK
	dsqpu, err := datastoredriver.QPU(depl.config)
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		server := QPUServer{config: depl.config, api: dsqpu}
		s := grpc.NewServer()
		pbQPU.RegisterQPUServer(s, &server)
		reflection.Register(s)
		lis, err := net.Listen("tcp", ":"+depl.config.Port)
		if err != nil {
			log.Fatal(err)
		}
		s.Serve(lis)
	}()

	for _, op := range depl.ops {
		dsqpu.Op(op)
	}
}

func deploy(depl *deployment) {
	createQPU(depl.config, depl)
}

func createQPU(conf []*config.Config, depl *deployment) {
	if len(conf) == 0 {
		return
	}
	var qpu QPUAPI
	var err error
	switch conf[0].QpuType {
	case pbQPU.ConfigResponse_DBDRIVER:
		conf[0].DatastoreConfig.Type = config.MOCK
		dsqpu, err := datastoredriver.QPU(conf[0])
		if err != nil {
			log.Fatal(err)
		}
		depl.datastoredriver = dsqpu
		qpu = dsqpu
	case pbQPU.ConfigResponse_FILTER:
		qpu, err = filter.QPU(conf[0])
		if err != nil {
			log.Fatal(err)
		}
	case pbQPU.ConfigResponse_CACHE:
		conf[0].CacheConfig.Size = 10
		qpu, err = cache.QPU(conf[0])
		if err != nil {
			log.Fatal(err)
		}
	}
	wait := startServer(conf[0], qpu, depl)
	<-wait
	createQPU(conf[1:], depl)
}

func startServer(conf *config.Config, api QPUAPI, depl *deployment) <-chan int {
	wait := make(chan int)
	go func() {
		server := QPUServer{config: conf, api: api}
		s := grpc.NewServer()
		pbQPU.RegisterQPUServer(s, &server)
		reflection.Register(s)
		lis, err := net.Listen("tcp", ":"+conf.Port)
		if err != nil {
			log.Fatal(err)
		}
		depl.servers = append(depl.servers, s)
		wait <- 0
		s.Serve(lis)
	}()
	return wait
}

func doTest(t *testing.T, test testScenario) {
	client, err := client.NewClient(test.endpoint)
	if err != nil {
		t.Error(err)
	}

	for _, q := range test.plan {
		stream, _, err := client.Query(
			"mock_bucket",
			q.query,
			protoutils.SnapshotTimePredicate(
				protoutils.SnapshotTime(pbUtils.SnapshotTime_LATEST, nil),
				protoutils.SnapshotTime(pbUtils.SnapshotTime_LATEST, nil),
			),
			nil,
			false,
		)
		if err != nil {
			t.Error(err)
		}
		streamRec, err := stream.Recv()
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, q.assertResponse, streamRec.GetLogOp().GetObjectId(), "")
	}
}
