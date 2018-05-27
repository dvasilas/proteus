package main

import (
	"bufio"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	pb "github.com/dimitriosvasilas/modqp/dataStore/datastore"

	"github.com/hpcloud/tail"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	port = ":50051"
)

type server struct {
	port    string
	datadir string
	logFile *os.File
}

func newDSServer(port string) server {
	os.Mkdir("../data", 0744)
	datapath, _ := filepath.Abs("../data")
	f, err := os.OpenFile(datapath+"/log.log", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0744)
	if err != nil {
		log.Fatalf("failed to create file: %v", err)
	}
	return server{port: port, datadir: datapath, logFile: f}
}

func createLogEntry(in *pb.PutObjMDRequest, ts string) string {
	logOp := ts + " " + in.Key
	for attr := range in.Attributes {
		logOp += " " + attr + " " + in.Attributes[attr]
	}
	return logOp + "\n"
}

func (s *server) writeToLog(in *pb.PutObjMDRequest, ts string) error {
	_, err := s.logFile.WriteString(createLogEntry(in, ts))
	if err != nil {
		log.Fatalf("failed to write to log: %v", err)
	}
	s.logFile.Sync()
	return nil
}

func inTimestampRange(timestampStr string, inpuTts int64, fn func(int64, int64) bool) (int64, bool, error) {
	timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
	if err != nil {
		return 0, false, err
	}
	return timestamp, fn(timestamp, inpuTts), nil
}

func (s *server) store(in *pb.PutObjMDRequest, ts string) error {
	f, err := os.OpenFile(s.datadir+"/"+in.Key+".log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(createLogEntry(in, ts))
	if err != nil {
		log.Fatalf("failed to write to log: %v", err)
	}
	s.logFile.Sync()
	return nil
}

func (s *server) materialize(key string, ts int64) (*pb.ObjectMD, error) {
	state := make(map[string]string)
	f, err := os.OpenFile(s.datadir+"/"+key+".log", os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var latestTs int64
	fScanner := bufio.NewScanner(f)
	for fScanner.Scan() {
		line := strings.Split(fScanner.Text(), " ")
		operationTs, inRange, err := inTimestampRange(line[0], ts, func(x, y int64) bool { return x <= y })
		if err != nil {
			return nil, err
		}
		if inRange {
			latestTs = operationTs
			if len(line) > 2 {
				attrs := line[2:]
				for i := 0; i < len(attrs); i++ {
					if i%2 == 0 {
						state[attrs[i]] = attrs[i+1]
					}
				}
			} else {
				state = make(map[string]string)
			}
		}
	}
	return &pb.ObjectMD{Key: key, State: state, Timestamp: latestTs}, nil
}

func (s *server) snapshotProducer(ts int64, msg chan *pb.ObjectMD) {
	files, err := ioutil.ReadDir(s.datadir)
	if err != nil {
		log.Fatalf("failed to read dir: %v", err)
	}
	for _, f := range files {
		if f.Name() != "log.log" {
			object, _ := s.materialize(f.Name(), ts)
			msg <- object
		}
	}
	msg <- nil
}

func (s *server) PutObjectMD(ctx context.Context, in *pb.PutObjMDRequest) (*pb.PutObjMDReply, error) {
	if in.Key == "" {
		return &pb.PutObjMDReply{Message: "error_key_empty"}, nil
	}
	ts := time.Now().UnixNano()
	s.writeToLog(in, strconv.FormatInt(ts, 10))
	s.store(in, strconv.FormatInt(ts, 10))
	return &pb.PutObjMDReply{Message: "OK", Timestamp: ts}, nil
}

func (s *server) GetObjectMD(ctx context.Context, in *pb.GetObjMDRequest) (*pb.GetObjMDReply, error) {
	state, _ := s.materialize(in.Key, in.Timestamp)
	return &pb.GetObjMDReply{Message: "OK", Object: state}, nil
}

func (s *server) GetSnapshot(in *pb.SubscribeRequest, stream pb.DataStore_GetSnapshotServer) error {
	msg := make(chan *pb.ObjectMD)
	go s.snapshotProducer(in.Timestamp, msg)
	for {
		if object := <-msg; object == nil {
			break
		} else if err := stream.Send(object); err != nil {
			return err
		}
	}
	return nil
}

func (s *server) SubscribeOps(in *pb.SubscribeRequest, stream pb.DataStore_SubscribeOpsServer) error {
	msg := make(chan *pb.ObjectMD)
	go s.snapshotProducer(in.Timestamp, msg)
	for {
		if object := <-msg; object == nil {
			break
		} else if err := stream.Send(&pb.StreamMsg{Type: "state", State: object}); err != nil {
			return err
		}
	}
	t, _ := tail.TailFile(s.datadir+"log.log", tail.Config{Follow: true})
	for line := range t.Lines {
		opLine := strings.Split(line.Text, " ")
		operationTs, inRange, err := inTimestampRange(opLine[0], in.Timestamp, func(x, y int64) bool { return x > y })
		if err != nil {
			return err
		}
		if inRange {
			op := &pb.Operation{Key: opLine[1], Timestamp: operationTs}
			if len(opLine) > 2 {
				op = &pb.Operation{Key: opLine[1], MdKey: opLine[2], MdValue: opLine[3], Timestamp: operationTs}
			}
			if err := stream.Send(&pb.StreamMsg{Type: "operation", Operation: op}); err != nil {
				return err
			}
		}
	}
	return nil
}

func main() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	server := newDSServer(port)
	pb.RegisterDataStoreServer(s, &server)

	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
