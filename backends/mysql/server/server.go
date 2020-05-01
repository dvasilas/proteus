package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"

	pb "github.com/dvasilas/proteus/backends/mysql/server/datastore"
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

// SubscribeToUpdates ...2
func (s *datastoreGRPCServer) SubscribeToUpdates(stream pb.PublishUpdates_SubscribeToUpdatesServer) error {
	_, err := stream.Recv()
	if err != nil {
		return err
	}

	seqID := int64(0)
	notificationCh := make(chan string)
	s.acitveConnections = append(s.acitveConnections, notificationCh)

	for updateMsg := range notificationCh {
		// fmt.Println(updateMsg)
		stream.Send(
			&pb.NotificationStream{
				SequenceId: seqID,
				Payload:    updateMsg,
			},
		)
		seqID++
	}
	return nil
}

func (s *datastoreGRPCServer) publishUpdates(updateCh chan string) {
	for updateMsg := range updateCh {
		fmt.Printf("publishUpdate: %s\n", updateMsg)
		for _, subscriberCh := range s.acitveConnections {
			subscriberCh <- updateMsg
		}
	}
}

func processEvents(w *fsnotify.Watcher, updateCh chan string, errs chan error) {
	fmt.Println("processEvents")
	for {
		select {
		case event := <-w.Events:
			fmt.Println("case event")
			fmt.Println(event.Name)
			fmt.Println(event.Op.String())
			if event.Op.String() == "WRITE" {
				// updateRecordF, err := os.Stat(event.Name)
				// if err != nil {
				// 	// done <- true
				// 	errs <- err
				// 	break
				// }
				data, err := ioutil.ReadFile(event.Name)
				if err != nil {
					errs <- err
					break
				}
				updateCh <- string(data)
			}
			// done <- false
			// msg <- &pbQPU.Operation{
			// 	Key: event.Name,
			// 	Op:  event.Op.String(),
			// 	Object: &pbQPU.Object{
			// 		Key: f.Name(),
			// 		Attributes: map[string]*pbQPU.Value{
			// 			"size":    utils.ValInt(f.Size()),
			// 			"mode":    utils.ValInt(int64(f.Mode())),
			// 			"modTime": utils.ValInt(f.ModTime().UnixNano()),
			// 		},
			// 	},
			// }
		case err := <-w.Errors:
			fmt.Println("case err")
			fmt.Println(err)
			// done <- true
			errs <- err
			break
		}
	}
}

func subscribeUpdatesFS(updateCh chan string, errs chan error) error {
	notifyPath := "/opt/trigger_data/votes"
	if _, err := os.Stat(notifyPath); os.IsNotExist(err) {
		os.Mkdir(notifyPath, os.ModeDir)
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer watcher.Close()

	go processEvents(watcher, updateCh, errs)

	err = watcher.Add(notifyPath)
	if err != nil {
		return err
	}
	<-errs
	return nil
}

func main() {
	server := datastoreGRPCServer{
		acitveConnections: make([]chan string, 0),
	}

	conf := config{
		Port: "50000",
	}

	s := grpc.NewServer()
	pb.RegisterPublishUpdatesServer(s, &server)
	reflection.Register(s)

	lis, err := net.Listen("tcp", ":"+conf.Port)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("listening")
	go s.Serve(lis)

	// http.HandleFunc("/hello", hello)
	// http.HandleFunc("/headers", headers)

	// fmt.Println("test")
	errs := make(chan error)
	updateCh := make(chan string)

	go server.publishUpdates(updateCh)

	err = subscribeUpdatesFS(updateCh, errs)
	if err != nil {
		log.Fatal(err)
	}
	err = <-errs
	fmt.Println(err)
	// log.Fatal(http.ListenAndServe(":8090", nil))
}
