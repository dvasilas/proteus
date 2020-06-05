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
	activeConnections map[string]chan string
}

func main() {
	port, ok := os.LookupEnv("PROTEUS_PUBLISH_PORT")
	if !ok {
		port = "50000"
	}

	tables := []string{"stories", "votes"}

	server := &datastoreGRPCServer{
		activeConnections: make(map[string]chan string),
	}

	s := grpc.NewServer()
	mysql.RegisterPublishUpdatesServer(s, server)
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

		// go server.publishUpdates(table, updateCh, errCh)
		go server.subscribeUpdatesFS("/opt/proteus-mysql/"+table, table, updateCh, errCh)

		go func() {
			for err := range errCh {
				if err != nil {
					log.Fatal(err)
				}
			}
		}()
	}

	s.Serve(lis)
}

// SubscribeToUpdates ...
func (s *datastoreGRPCServer) SubscribeToUpdates(stream pb.PublishUpdates_SubscribeToUpdatesServer) error {
	request, err := stream.Recv()
	if err != nil {
		return err
	}

	table := request.GetRequest().GetTable()

	seqID := int64(0)
	notificationCh := make(chan string)
	s.activeConnections[table] = notificationCh

	for updateMsg := range s.activeConnections[table] {
		var update MySQLUpdate
		if err := json.Unmarshal([]byte(updateMsg), &update); err != nil {
			return err
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
			return err
		}
		timestamp, err := ptypes.TimestampProto(time.Unix(ts, 0))
		if err != nil {
			return err
		}

		err = stream.Send(&pb.UpdateRecord{
			SequenceId: seqID,
			RecordID:   update.RecordID,
			Table:      update.Table,
			Attributes: attributes,
			Timestamp:  timestamp,
		},
		)
		if err != nil {
			return err
		}
		seqID++
	}

	return nil
}

func (s *datastoreGRPCServer) subscribeUpdatesFS(logPath, table string, updateCh chan string, errCh chan error) {
	_, err := os.Stat(logPath)
	if os.IsNotExist(err) {
		os.Mkdir(logPath, os.ModeDir)
	} else if err != nil {
		errCh <- err
		return
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		errCh <- err
		return
	}

	err = watcher.Add(logPath)
	if err != nil {
		errCh <- err
		return
	}

	for {
		select {
		case event := <-watcher.Events:
			if event.Op.String() == "WRITE" {
				data, err := ioutil.ReadFile(event.Name)
				if err != nil {
					errCh <- err
					return
				}
				if ch, found := s.activeConnections[table]; found {
					ch <- string(data)
				}
			}
		case err := <-watcher.Errors:
			errCh <- err
			return
		}
	}

}
