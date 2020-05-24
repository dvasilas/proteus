package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"

	mysql "github.com/dvasilas/proteus-mysql-notifications/proto"
	"github.com/fsnotify/fsnotify"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type datastoreGRPCServer struct {
	acitveConnections map[string]chan string
}

// SubscribeToUpdates ...
func (s *datastoreGRPCServer) SubscribeToUpdates(stream mysql.PublishUpdates_SubscribeToUpdatesServer) error {
	request, err := stream.Recv()
	if err != nil {
		return err
	}
	table := request.GetRequest().GetTable()

	seqID := int64(0)
	notificationCh := make(chan string)
	s.acitveConnections[table] = notificationCh

	for updateMsg := range notificationCh {
		stream.Send(
			&mysql.NotificationStream{
				SequenceId: seqID,
				Payload:    updateMsg,
			},
		)
		seqID++
	}
	return nil
}

func subscribeUpdatesFS(logPath string, updateCh chan string, errs chan error) error {
	fmt.Println(logPath)
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
	// <-errs
	return nil
}

func processEvents(w *fsnotify.Watcher, updateCh chan string, errs chan error) {
	for {
		fmt.Println("processEvents loop")
		select {
		case event := <-w.Events:
			fmt.Println(event)
			if event.Op.String() == "WRITE" {
				data, err := ioutil.ReadFile(event.Name)
				if err != nil {
					errs <- err
					break
				}
				updateCh <- string(data)
			}
		case err := <-w.Errors:
			fmt.Println(err)
			errs <- err
			break
		}
	}
}

func (s *datastoreGRPCServer) publishUpdates(table string, updateCh chan string, errCh chan error) {
	for updateMsg := range updateCh {
		if ch, found := s.acitveConnections[table]; found {
			ch <- updateMsg
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
		acitveConnections: make(map[string]chan string, 0),
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
