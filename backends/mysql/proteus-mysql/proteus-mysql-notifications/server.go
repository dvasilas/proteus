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

type config struct {
	Port string
}

type datastoreGRPCServer struct {
	acitveConnections []chan string
}

// SubscribeToUpdates ...
func (s *datastoreGRPCServer) SubscribeToUpdates(stream mysql.PublishUpdates_SubscribeToUpdatesServer) error {
	_, err := stream.Recv()
	if err != nil {
		return err
	}

	seqID := int64(0)
	notificationCh := make(chan string)
	s.acitveConnections = append(s.acitveConnections, notificationCh)

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
	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		os.Mkdir(logPath, os.ModeDir)
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer watcher.Close()

	go processEvents(watcher, updateCh, errs)

	err = watcher.Add(logPath)
	if err != nil {
		return err
	}
	<-errs
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

func (s *datastoreGRPCServer) publishUpdates(updateCh chan string) {
	for updateMsg := range updateCh {
		for _, subscriberCh := range s.acitveConnections {
			subscriberCh <- updateMsg
		}
	}
}

func main() {
	var ok bool
	var port, path string
	if port, ok = os.LookupEnv("PROTEUS_PUBLISH_PORT"); !ok {
		port = "50000"
	}

	if path, ok = os.LookupEnv("PROTEUS_LOG_PATH"); !ok {
		path = "/opt/proteus-mysql/votes"
	}

	fmt.Println(port, path)

	conf := config{
		Port: port,
	}

	server := datastoreGRPCServer{
		acitveConnections: make([]chan string, 0),
	}

	s := grpc.NewServer()
	mysql.RegisterPublishUpdatesServer(s, &server)
	reflection.Register(s)

	lis, err := net.Listen("tcp", ":"+conf.Port)
	if err != nil {
		log.Fatal(err)
	}
	go s.Serve(lis)

	errs := make(chan error)
	updateCh := make(chan string)

	go server.publishUpdates(updateCh)

	err = subscribeUpdatesFS(path, updateCh, errs)
	if err != nil {
		log.Fatal(err)
	}
	err = <-errs
}
