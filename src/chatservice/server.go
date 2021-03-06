package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/google/uuid"
	c "github.com/richardjaytea/infipic/config"
	"io"
	"log"
	"net"
	"strings"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/richardjaytea/infipic/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/examples/data"
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
	serverAddrRoom     = flag.String("server_addr_room", "localhost:10003", "The server address for the room service server")
	serverHostOverride = flag.String("server_host_override", "x.test.youtube.com", "The server name used to verify the hostname returned by the TLS handshake")
)

var (
	id        = "service-" + uuid.NewString()
	rooms     []string
	roomWords map[string][]string
	userWords map[string][]string
	userNames map[string]string
)

type messageStreamMap map[string]*pb.Chat_GetMessagesServer

type chatServer struct {
	pb.UnimplementedChatServer
	roomChatStreams map[string]messageStreamMap
	imageClient     pb.ImageClient
	roomClient      pb.RoomClient
}

func (s *chatServer) GetMessages(m *pb.MessageStreamRequest, stream pb.Chat_GetMessagesServer) error {
	s.roomChatStreams[m.RoomKey][m.Id] = &stream
	userNames[m.Id] = m.Name
	s.broadcastMessage(m.RoomKey, buildMessageResponse(c.VGetEnv("SYS_CHAT_NAME"), fmt.Sprintf("Welcome %s!", m.Name)))
	log.Printf("Added Stream: %s", m.Id)
	s.keepAliveTillClose(m.Id, m.RoomKey)
	return nil
}

func (s *chatServer) SendMessage(ctx context.Context, message *pb.MessageRequest) (*pb.MatchWordResponse, error) {
	m := strings.ToLower(strings.TrimSpace(message.Content))

	if contains(roomWords[message.RoomKey], m) {
		stream := s.roomChatStreams[message.RoomKey][message.Id]
		if contains(userWords[message.Id], m) {
			(*stream).Send(buildMessageResponse(c.VGetEnv("SYS_CHAT_NAME"), "You have already correctly guessed this word!"))
		} else {
			userWords[message.Id] = append(userWords[message.Id], m)
			(*stream).Send(buildMessageResponse(c.VGetEnv("SYS_CHAT_NAME"), "Your guess is correct!"))
			return &pb.MatchWordResponse{Match: true}, nil
		}
	} else {
		response := buildMessageResponse(userNames[message.Id], message.Content)
		s.broadcastMessage(message.RoomKey, response)
	}

	return &pb.MatchWordResponse{Match: false}, nil
}

func (s *chatServer) broadcastMessage(roomKey string, m *pb.MessageResponse) {
	for _, stream := range s.roomChatStreams[roomKey] {
		if stream != nil && *stream != nil {
			if err := (*stream).Send(m); err != nil {
				log.Println(err)
			}
		}
	}
}

func buildMessageResponse(name, content string) *pb.MessageResponse {
	return &pb.MessageResponse{
		Name:      name,
		Content:   content,
		Timestamp: time.Now().Format(time.RFC822),
	}
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
		stream, err := s.imageClient.GetImageAndWords(
			context.Background(),
			&pb.Client{
				Id:      id,
				RoomKey: v,
			})
		if err != nil {
			log.Fatalf("%v.GetWord(_) = _, %v", s.imageClient, err)
		}
		go keepWordUpdated(stream, v)
	}
}

func (s *chatServer) getRooms() {
	r, err := s.roomClient.GetRooms(context.Background(), &empty.Empty{})
	if err != nil {
		log.Fatal("error in trying to get rooms from room service")
		return
	}

	for _, v := range r.Rooms {
		rooms = append(rooms, v.GetKey())
	}
}

func keepWordUpdated(stream pb.Image_GetImageAndWordsClient, roomKey string) {
	for {
		word, err := stream.Recv()

		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("keepWordUpdated(_) = _, %v", err)
		}

		clearWords(roomKey)
		roomWords[roomKey] = word.GetWords()
	}
}

func clearWords(roomKey string) {
	var s []string
	delete(roomWords, roomKey)
	roomWords[roomKey] = s

	for i, _ := range userWords {
		userWords[i] = nil
	}
}

func contains(s []string, c string) bool {
	for _, v := range s {
		if v == c {
			return true
		}
	}

	return false
}

func newServer() *chatServer {
	s := &chatServer{
		roomChatStreams: make(map[string]messageStreamMap),
	}
	roomWords = make(map[string][]string)
	userWords = make(map[string][]string)
	userNames = make(map[string]string)

	for _, v := range rooms {
		s.roomChatStreams[v] = messageStreamMap{}
	}

	s.connectServices()

	for _, v := range rooms {
		s.roomChatStreams[v] = messageStreamMap{}
	}

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
	conn, err := grpc.Dial(*serverAddrRoom, opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}

	s.roomClient = pb.NewRoomClient(conn)
	s.getRooms()

	conn, err = grpc.Dial(*serverAddrImage, opts...)
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
