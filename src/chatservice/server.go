package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/google/uuid"
	"github.com/richardjaytea/infipic/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/examples/data"
	"log"
	"net"
	"time"
)

var (
	tls        = flag.Bool("tls", false, "Connection uses TLS if true, else plain TCP")
	certFile   = flag.String("cert_file", "", "The TLS cert file")
	keyFile    = flag.String("key_file", "", "The TLS key file")
	jsonDBFile = flag.String("json_db_file", "", "A json file containing a list of features")
	port       = flag.Int("port", 10000, "The server port")
	emp        = empty.Empty{}
)

type chatServer struct {
	pb.UnimplementedChatServer
	messageStreams map[string]pb.Chat_GetMessagesServer
}

func (s *chatServer) ConnectChat(ctx context.Context, empty *empty.Empty) (*pb.Client, error) {
	id := uuid.NewString()
	s.messageStreams[id] = nil
	log.Printf("New Client Connection: %s", id)
	s.broadcastWelcomeMessage(ctx, id)
	return &pb.Client{Id: id}, nil
}

func (s *chatServer) GetMessages(client *pb.Client, stream pb.Chat_GetMessagesServer) error {
	s.messageStreams[client.Id] = stream
	log.Printf("Added Stream: %s", client.Id)
	s.keepAliveTillClose(client.Id)
	return nil
}

func (s *chatServer) SendMessage(ctx context.Context, message *pb.MessageRequest) (*empty.Empty, error) {
	log.Println("Broadcasting Message")
	response := &pb.MessageResponse{
		Id:        message.Id,
		Content:   message.Content,
		Timestamp: time.Now().Format(time.RFC822),
	}

	for _, stream := range s.messageStreams {
		if stream != nil {
			if err := stream.Send(response); err != nil {
				log.Println(err)
				return &emp, err
			}
		}
	}

	return &emp, nil
}

func (s *chatServer) broadcastWelcomeMessage(ctx context.Context, id string) {
	_, _ = s.SendMessage(
		ctx,
		&pb.MessageRequest{
			Id:      id,
			Content: fmt.Sprintf("Welcome %s", id),
		},
	)
}

func (s *chatServer) keepAliveTillClose(id string) {
	stream := s.messageStreams[id]
	select {
	case <-stream.Context().Done():
		// TODO: Lock map for deletion?
		delete(s.messageStreams, id)
		log.Printf("Connection Disconnected: %s", id)
		return
	}
}

func newServer() *chatServer {
	s := &chatServer{
		messageStreams: make(map[string]pb.Chat_GetMessagesServer),
	}
	return s
}

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	if *tls {
		if *certFile == "" {
			*certFile = data.Path("x509/server_cert.pem")
		}
		if *keyFile == "" {
			*keyFile = data.Path("x509/server_key.pem")
		}
		creds, err := credentials.NewServerTLSFromFile(*certFile, *keyFile)
		if err != nil {
			log.Fatalf("Failed to generate credentials %v", err)
		}
		opts = []grpc.ServerOption{grpc.Creds(creds)}
	}
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterChatServer(grpcServer, newServer())
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve Chat: %v", err)
	}
}
