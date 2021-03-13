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
	"io"
	"log"
	"net"
	"time"
)

var (
	tls                = flag.Bool("tls", false, "Connection uses TLS if true, else plain TCP")
	certFile           = flag.String("cert_file", "", "The TLS cert file")
	keyFile            = flag.String("key_file", "", "The TLS key file")
	jsonDBFile         = flag.String("json_db_file", "", "A json file containing a list of features")
	port               = flag.Int("port", 10000, "The server port")
	emp                = empty.Empty{}
	caFile             = flag.String("ca_file", "", "The file containing the CA root cert file")
	serverAddrImage    = flag.String("server_addr_image", "localhost:10001", "The server address for the image service server")
	serverHostOverride = flag.String("server_host_override", "x.test.youtube.com", "The server name used to verify the hostname returned by the TLS handshake")
)

var (
	rooms    = []string{"test_room"}
	roomWord map[string]string
)

type messageStreamMap map[string]*pb.Chat_GetMessagesServer

type chatServer struct {
	pb.UnimplementedChatServer
	roomChatStreams map[string]messageStreamMap
	imageClient     pb.ImageClient
}

func (s *chatServer) ConnectChat(ctx context.Context, r *pb.Room) (*pb.Client, error) {
	id := uuid.NewString()
	s.roomChatStreams[r.Key][id] = nil

	log.Printf("New Client Connection: %s", id)
	s.broadcastWelcomeMessage(ctx, id, r.Key)
	return &pb.Client{Id: id, RoomKey: r.Key}, nil
}

func (s *chatServer) GetMessages(client *pb.Client, stream pb.Chat_GetMessagesServer) error {
	s.roomChatStreams[client.RoomKey][client.Id] = &stream
	log.Printf("Added Stream: %s", client.Id)
	s.keepAliveTillClose(client.Id, client.RoomKey)
	return nil
}

func (s *chatServer) SendMessage(ctx context.Context, message *pb.MessageRequest) (*empty.Empty, error) {
	if message.Content == roomWord[message.RoomKey] {
		log.Println("Correct Guess!")
		stream := s.roomChatStreams[message.RoomKey][message.Id]
		(*stream).Send(buildMessageResponse("Server ID", "Your guess is correct!"))
	} else {
		log.Println("Broadcasting Message")
		response := buildMessageResponse(message.Id, message.Content)
		for _, stream := range s.roomChatStreams[message.RoomKey] {
			if stream != nil && *stream != nil {
				if err := (*stream).Send(response); err != nil {
					log.Println(err)
					return &emp, err
				}
			}
		}
	}

	return &emp, nil
}

func buildMessageResponse(id string, content string) *pb.MessageResponse {
	return &pb.MessageResponse{
		Id:        id,
		Content:   content,
		Timestamp: time.Now().Format(time.RFC822),
	}
}

func (s *chatServer) broadcastWelcomeMessage(ctx context.Context, id string, roomKey string) {
	_, _ = s.SendMessage(
		ctx,
		&pb.MessageRequest{
			Id:      id,
			RoomKey: roomKey,
			Content: fmt.Sprintf("Welcome %s", id),
		},
	)
}

func (s *chatServer) keepAliveTillClose(id string, roomKey string) {
	stream := *s.roomChatStreams[roomKey][id]
	select {
	case <-stream.Context().Done():
		// TODO: Lock map for deletion?
		delete(s.roomChatStreams[roomKey], id)
		log.Printf("Connection Disconnected: %s", id)
		return
	}
}

func (s *chatServer) getImageWord() {
	for _, v := range rooms {
		stream, err := s.imageClient.GetWord(
			context.Background(),
			&pb.Room{
				Key: v,
			})
		if err != nil {
			log.Fatalf("%v.GetWord(_) = _, %v", s.imageClient, err)
		}
		go keepWordUpdated(stream, v)
	}
}

func keepWordUpdated(stream pb.Image_GetWordClient, roomKey string) {
	for {
		word, err := stream.Recv()

		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("keepWordUpdated(_) = _, %v", err)
		}

		roomWord[roomKey] = word.GetWord()
		log.Printf("Got Word: %s", roomWord[roomKey])
		log.Println(roomWord)
	}
}

func newServer() *chatServer {
	s := &chatServer{
		roomChatStreams: make(map[string]messageStreamMap),
	}
	roomWord = make(map[string]string)

	for _, v := range rooms {
		s.roomChatStreams[v] = messageStreamMap{}
	}

	go s.connectServices()

	return s
}

func (s *chatServer) connectServices() {
	flag.Parse()
	var opts []grpc.DialOption
	if *tls {
		if *caFile == "" {
			*caFile = data.Path("x509/ca_cert.pem")
		}
		creds, err := credentials.NewClientTLSFromFile(*caFile, *serverHostOverride)
		if err != nil {
			log.Fatalf("Failed to create TLS credentials %v", err)
		}
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		opts = append(opts, grpc.WithInsecure())
	}

	opts = append(opts, grpc.WithBlock())
	conn, err := grpc.Dial(*serverAddrImage, opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	// defer conn.Close()
	s.imageClient = pb.NewImageClient(conn)
	s.getImageWord()
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
