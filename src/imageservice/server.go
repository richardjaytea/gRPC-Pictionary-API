package main

import (
	"flag"
	"fmt"
	"github.com/golang/protobuf/ptypes/empty"
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
	port       = flag.Int("port", 10001, "The server port")
	emp        = empty.Empty{}
)

var (
	// TODO: room_key should be coming from database
	rooms     = []string{"test_room"}
	roomImage map[string]string // map[string]bytes?
	roomWord  map[string]string
)

type imageServer struct {
	pb.UnimplementedImageServer
	roomImageStreams map[string]*pb.Image_GetImageServer
	roomWordStreams  map[string]*pb.Image_GetWordServer
}

func (s *imageServer) GetWord(r *pb.Room, stream pb.Image_GetWordServer) error {
	s.roomWordStreams[r.Key] = &stream
	log.Printf("Word Stream Created: %s", r.Key)
	select {
	case <-stream.Context().Done():
		// TODO: Lock map for deletion?
		delete(s.roomWordStreams, r.Key)
		log.Printf("Word Connection Disconnected: %s", r.Key)
		return nil
	}
}

func (s *imageServer) GetImage(r *pb.Room, stream pb.Image_GetImageServer) error {
	s.roomImageStreams[r.Key] = &stream
	log.Printf("Image Stream Created: %s", r.Key)
	select {
	case <-stream.Context().Done():
		// TODO: Lock map for deletion?
		delete(s.roomImageStreams, r.Key)
		log.Printf("Image Connection Disconnected: %s", r.Key)
		return nil
	}
}

func (s *imageServer) sendWord(roomKey string, word string) {
	time.Sleep(10 * time.Second)
	stream := *s.roomWordStreams[roomKey]
	if err := stream.Send(&pb.WordResponse{
		Word: word,
	}); err != nil {
		log.Println(err)
	}
}

func newServer() *imageServer {
	s := &imageServer{
		roomImageStreams: make(map[string]*pb.Image_GetImageServer),
		roomWordStreams:  make(map[string]*pb.Image_GetWordServer),
	}
	roomImage = make(map[string]string)
	roomWord = make(map[string]string)

	// test
	go s.sendWord(rooms[0], "cat")

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
	pb.RegisterImageServer(grpcServer, newServer())
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve Chat: %v", err)
	}
}
