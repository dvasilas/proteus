package main

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	pb "github.com/dvasilas/proteus-lobsters/proto"
	"github.com/golang/protobuf/ptypes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// MsgTable ...
type MsgTable struct {
	Table string
}

// VotesUpdate ...
type VotesUpdate struct {
	Table      string
	Ts         string
	Vote       string
	Story_id   string
	Comment_id string
}

// StoriesUpdate ...
type StoriesUpdate struct {
	Table       string
	Ts          string
	Story_id    string
	User_id     string
	Title       string
	Description string
	Short_id    string
}

// CommentsUpdate ...
type CommentsUpdate struct {
	Table      string
	Ts         string
	Comment_id string
	User_id    string
	Story_id   string
	Comment    string
}

var socketPort = 2048

type datastoreGRPCServer struct {
	activeConnections map[string]chan string
}

func main() {
	publishPort, ok := os.LookupEnv("PROTEUS_PUBLISH_PORT")
	if !ok {
		publishPort = "50000"
	}

	notificationServer, grpcServer, lis := notificationServer(publishPort)

	go socketServer(socketPort, notificationServer)

	log.Fatal(grpcServer.Serve(lis))
}

func notificationServer(port string) (*datastoreGRPCServer, *grpc.Server, net.Listener) {
	server := &datastoreGRPCServer{
		activeConnections: make(map[string]chan string),
	}

	s := grpc.NewServer()
	pb.RegisterPublishUpdatesServer(s, server)
	reflection.Register(s)

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}

	return server, s, lis
}

func socketServer(port int, s *datastoreGRPCServer) {
	listen, err := net.Listen("tcp4", ":"+strconv.Itoa(port))
	if err != nil {
		log.Fatalf("Socket listen port %d failed,%s", port, err)
		os.Exit(1)
	}

	defer listen.Close()

	log.Printf("Begin listen port: %d", port)

	for {
		conn, err := listen.Accept()
		if err != nil {
			log.Fatalln(err)
			continue
		}
		go handler(conn, s)
	}
}

func handler(conn net.Conn, s *datastoreGRPCServer) {
	defer conn.Close()

	var (
		buf = make([]byte, 1024)
		r   = bufio.NewReader(conn)
	)

ILOOP:
	for {
		n, err := r.Read(buf)
		data := string(buf[:n])

		switch err {
		case io.EOF:
			break ILOOP
		case nil:
			idx := strings.Index(data, "}")
			for idx != len(data)-1 {
				chunk := data[:idx+1]
				var msg MsgTable
				if err := json.Unmarshal([]byte(chunk), &msg); err != nil {
					log.Printf("json.Unmarshal failed:%s %s\n", err, chunk)
					os.Exit(1)
				}
				if ch, f := s.activeConnections[msg.Table]; f {
					ch <- chunk
				}
				data = data[idx+1:]
				idx = strings.Index(data, "}")
			}
		default:
			log.Fatalf("Receive data failed:%s", err)
			os.Exit(1)
		}

	}
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
		attributes := make([]*pb.Attributes, 0)
		var ts time.Time

		switch table {
		case "votes":
			var update VotesUpdate
			if err := json.Unmarshal([]byte(updateMsg), &update); err != nil {
				log.Printf("json.Unmarshal failed:%s %s\n", err, updateMsg)
				return err
			}

			ts, err = time.Parse("2006-01-02 15:04:05.000000", update.Ts)
			if err != nil {
				ts, err = time.Parse("2006-01-02 15:04:05", update.Ts)
				if err != nil {
					log.Fatalf("time.Parse failed:%s", err)
					return err
				}
			}

			attributes = append(attributes,
				&pb.Attributes{
					Key:      "vote",
					ValueNew: update.Vote,
				})

			attributes = append(attributes,
				&pb.Attributes{
					Key:      "story_id",
					ValueNew: update.Story_id,
				})

			if len(update.Comment_id) > 0 {
				attributes = append(attributes,
					&pb.Attributes{
						Key:      "comment_id",
						ValueNew: update.Comment_id,
					})
			}
		case "stories":
			var update StoriesUpdate
			if err := json.Unmarshal([]byte(updateMsg), &update); err != nil {
				log.Printf("json.Unmarshal failed:%s %s\n", err, updateMsg)
				return err
			}

			ts, err = time.Parse("2006-01-02 15:04:05.000000", update.Ts)
			if err != nil {
				ts, err = time.Parse("2006-01-02 15:04:05", update.Ts)
				if err != nil {
					log.Fatalf("time.Parse failed:%s", err)
					return err
				}
			}

			if len(update.Story_id) > 0 {
				attributes = append(attributes,
					&pb.Attributes{
						Key:      "id",
						ValueNew: update.Story_id,
					})
			}

			if len(update.Title) > 0 {
				attributes = append(attributes,
					&pb.Attributes{
						Key:      "title",
						ValueNew: update.Title,
					})
			}

			if len(update.Description) > 0 {
				attributes = append(attributes,
					&pb.Attributes{
						Key:      "description",
						ValueNew: update.Description,
					})
			}

			if len(update.Short_id) > 0 {
				attributes = append(attributes,
					&pb.Attributes{
						Key:      "short_id",
						ValueNew: update.Short_id,
					})
			}

		case "comments":
			var update CommentsUpdate
			if err := json.Unmarshal([]byte(updateMsg), &update); err != nil {
				log.Printf("json.Unmarshal failed:%s %s\n", err, updateMsg)
				return err
			}

			ts, err = time.Parse("2006-01-02 15:04:05.000000", update.Ts)
			if err != nil {
				ts, err = time.Parse("2006-01-02 15:04:05", update.Ts)
				if err != nil {
					log.Fatalf("time.Parse failed:%s", err)
					return err
				}
			}

			if len(update.Comment_id) > 0 {
				attributes = append(attributes,
					&pb.Attributes{
						Key:      "id",
						ValueNew: update.Comment_id,
					})
			}

			if len(update.User_id) > 0 {
				attributes = append(attributes,
					&pb.Attributes{
						Key:      "user_id",
						ValueNew: update.User_id,
					})
			}

			if len(update.Story_id) > 0 {
				attributes = append(attributes,
					&pb.Attributes{
						Key:      "story_id",
						ValueNew: update.Story_id,
					})
			}

			if len(update.Comment) > 0 {
				attributes = append(attributes,
					&pb.Attributes{
						Key:      "comment",
						ValueNew: update.Comment,
					})
			}

		default:
			continue
		}

		timestamp, err := ptypes.TimestampProto(ts)
		if err != nil {
			log.Fatalf("ptypes.TimestampProto failed:%s", err)
			return err
		}

		err = stream.Send(&pb.UpdateRecord{
			SequenceId: seqID,
			Table:      table,
			Attributes: attributes,
			Timestamp:  timestamp,
		})

		if err != nil {
			delete(s.activeConnections, table)
			break
		}
		seqID++
	}

	return nil
}
