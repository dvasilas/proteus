package main

import (
	"bufio"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	pb "modqp/data_store/datastore"

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

func createLogEntry(in *pb.PutObjMDRequest, ts string) string {
	logOp := ts + " " + in.Key
	if in.MdKey != "" {
		logOp += " " + in.MdKey
	}
	if in.MdValue != "" {
		logOp += " " + in.MdValue
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
	f, err := os.OpenFile(s.datadir+in.Key, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("failed to create file: %v", err)
	}
	defer f.Close()
	_, err = f.WriteString(createLogEntry(in, ts))
	if err != nil {
		log.Fatalf("failed to write to log: %v", err)
	}
	s.logFile.Sync()
	return nil
}

func newDSServer(port string) server {
	pwd, err := os.Getwd()
	if err != nil {
		log.Fatalf("Getwd failed: %v", err)
	}
	datapath := pwd + "/data/"
	f, err := os.OpenFile(datapath+"log", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Fatalf("failed to create file: %v", err)
	}
	return server{port: port, datadir: datapath, logFile: f}
}

func (s *server) PutObjectMD(ctx context.Context, in *pb.PutObjMDRequest) (*pb.PutObjMDReply, error) {
	ts := time.Now().UnixNano()
	s.writeToLog(in, strconv.FormatInt(ts, 10))
	s.store(in, strconv.FormatInt(ts, 10))
	return &pb.PutObjMDReply{Message: "OK", Timestamp: ts}, nil
}

func (s *server) Materialize(key string, ts int64) (*pb.ObjectMD, error) {
	state := make(map[string]string)
	f, _ := os.Open(s.datadir + key)
	defer f.Close()
	var latestTs int64
	fScanner := bufio.NewScanner(f)
	for fScanner.Scan() {
		line := strings.Split(fScanner.Text(), " ")
		if len(line) > 2 {
			operationTs, inRange, err := inTimestampRange(line[0], ts, func(x, y int64) bool { return x <= y })
			if err != nil {
				return nil, err
			}
			if inRange {
				latestTs = operationTs
				state[line[2]] = line[3]
			}
		}
	}
	return &pb.ObjectMD{Key: key, State: state, Timestamp: latestTs}, nil
}

func (s *server) GetObjectMD(ctx context.Context, in *pb.GetObjMDRequest) (*pb.GetObjMDReply, error) {
	state, _ := s.Materialize(in.Key, in.Timestamp)
	return &pb.GetObjMDReply{Message: "OK", Object: state}, nil
}

func (s *server) GetOperations(in *pb.SubscribeRequest, stream pb.DataStore_GetOperationsServer) error {
	f, err := os.Open(s.datadir + "log")
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
		return err
	}
	defer f.Close()
	fScanner := bufio.NewScanner(f)
	for fScanner.Scan() {
		line := strings.Split(fScanner.Text(), " ")
		operationTs, inRange, err := inTimestampRange(line[0], in.Timestamp, func(x, y int64) bool { return x > y })
		if err != nil {
			return err
		}
		if inRange {
			op := &pb.Operation{Key: line[1], Timestamp: operationTs}
			if len(line) > 2 {
				op = &pb.Operation{Key: line[1], MdKey: line[2], MdValue: line[3], Timestamp: operationTs}
			}
			if err := stream.Send(op); err != nil {
				return err
			}
		}
	}
	return nil
}
func (s *server) snapshotProducer(ts int64, msg chan *pb.ObjectMD) {
	files, err := ioutil.ReadDir(s.datadir)
	if err != nil {
		log.Fatalf("failed to read dir: %v", err)
	}
	for _, f := range files {
		if f.Name() != "log" {
			object, _ := s.Materialize(f.Name(), ts)
			msg <- object
		}
	}
	msg <- nil
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

func (s *server) Subscribe(in *pb.SubscribeRequest, stream pb.DataStore_SubscribeServer) error {
	msg := make(chan *pb.ObjectMD)
	go s.snapshotProducer(in.Timestamp, msg)
	for {
		if object := <-msg; object == nil {
			break
		} else if err := stream.Send(&pb.StreamMsg{Type: "state", State: object}); err != nil {
			return err
		}
	}
	t, _ := tail.TailFile(s.datadir+"log", tail.Config{Follow: true})
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
