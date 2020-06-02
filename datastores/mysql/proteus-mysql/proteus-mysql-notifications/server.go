package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	mysql "github.com/dvasilas/proteus-mysql-notifications/proto"
	pb "github.com/dvasilas/proteus-mysql-notifications/proto"
	"github.com/fsnotify/fsnotify"
	ptypes "github.com/golang/protobuf/ptypes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// MySQLUpdate ...
type MySQLUpdate struct {
	RecordID   string
	Table      string
	Timestamp  string
	Attributes []struct {
		Key      string
		ValueOld string
		ValueNew string
	}
}

type datastoreGRPCServer struct {
	activeConnections map[string]chan *pb.UpdateRecord
}

// SubscribeToUpdates ...
func (s *datastoreGRPCServer) SubscribeToUpdates(stream pb.PublishUpdates_SubscribeToUpdatesServer) error {
	request, err := stream.Recv()
	if err != nil {
		return err
	}
	table := request.GetRequest().GetTable()

	seqID := int64(0)
	notificationCh := make(chan *pb.UpdateRecord)
	s.activeConnections[table] = notificationCh

	for update := range notificationCh {
		update.SequenceId = seqID
		stream.Send(update)
		seqID++
	}
	return nil
}

func subscribeUpdatesFS(logPath string, updateCh chan string, errs chan error) error {
	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		os.Mkdir(logPath, os.ModeDir)
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	// defer watcher.Close()

	go processEvents(watcher, updateCh, errs)

	err = watcher.Add(logPath)
	if err != nil {
		return err
	}
	return nil
}

func processEvents(w *fsnotify.Watcher, updateCh chan string, errs chan error) {
	for {
		select {
		case event := <-w.Events:
			if event.Op.String() == "WRITE" {
				data, err := ioutil.ReadFile(event.Name)
				if err != nil {
					errs <- err
					break
				}
				updateCh <- string(data)
			}
		case err := <-w.Errors:
			errs <- err
			break
		}
	}
}

func (s *datastoreGRPCServer) publishUpdates(table string, updateCh chan string, errCh chan error) {
	for updateMsg := range updateCh {
		var update MySQLUpdate
		if err := json.Unmarshal([]byte(updateMsg), &update); err != nil {
			errCh <- err
		}
		attributes := make([]*pb.Attributes, len(update.Attributes))
		for i, entry := range update.Attributes {
			attributes[i] = &pb.Attributes{
				Key:      entry.Key,
				ValueNew: entry.ValueNew,
			}
		}
		ts, err := strconv.ParseInt(update.Timestamp, 10, 64)
		if err != nil {
			errCh <- err
		}
		timestamp, err := ptypes.TimestampProto(time.Unix(ts, 0))
		if err != nil {
			errCh <- err
		}

		if ch, found := s.activeConnections[table]; found {
			ch <- &pb.UpdateRecord{
				RecordID:   update.RecordID,
				Table:      update.Table,
				Attributes: attributes,
				Timestamp:  timestamp,
			}
		}
	}
}

func main() {
	port, ok := os.LookupEnv("PROTEUS_PUBLISH_PORT")
	if !ok {
		port = "50000"
	}

	tables := []string{"stories", "votes"}

	server := datastoreGRPCServer{
		activeConnections: make(map[string]chan *pb.UpdateRecord, 0),
	}

	s := grpc.NewServer()
	mysql.RegisterPublishUpdatesServer(s, &server)
	reflection.Register(s)

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatal(err)
	}

	var errCh chan error
	var updateCh chan string
	for _, table := range tables {
		errCh = make(chan error)
		updateCh = make(chan string)

		go server.publishUpdates(table, updateCh, errCh)

		err = subscribeUpdatesFS("/opt/proteus-mysql/"+table, updateCh, errCh)
		if err != nil {
			log.Fatal(err)
		}
	}

	s.Serve(lis)
}
