package main

import (
	"encoding/base64"
	"flag"
	"fmt"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/richardjaytea/infipic/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/examples/data"
	"io/ioutil"
	"log"
	"net"
	"net/http"
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
	roomImage map[string]string
	roomWord  map[string]string
)

type imageStreams []*pb.Image_GetImageServer

type imageServer struct {
	pb.UnimplementedImageServer
	roomImageStreams map[string]imageStreams
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
	s.roomImageStreams[r.Key] = append(s.roomImageStreams[r.Key], &stream)
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

func (s *imageServer) sendImage(roomKey string) {
	for _, stream := range s.roomImageStreams[roomKey] {
		if stream != nil && *stream != nil {
			if err := (*stream).Send(&pb.ImageResponse{
				Content: roomImage[roomKey],
			}); err != nil {
				log.Println(err)
			}
		}
	}
}

func newServer() *imageServer {
	s := &imageServer{
		roomImageStreams: make(map[string]imageStreams),
		roomWordStreams:  make(map[string]*pb.Image_GetWordServer),
	}
	roomImage = make(map[string]string)
	roomWord = make(map[string]string)

	// test
	go s.sendWord(rooms[0], "cat")
	go s.testGetImage(rooms[0], "https://static.scientificamerican.com/sciam/cache/file/32665E6F-8D90-4567-9769D59E11DB7F26_source.jpg?w=590&h=800&7E4B4CAD-CAE1-4726-93D6A160C2B068B2")

	return s
}

func (s *imageServer) testGetImage(roomKey, url string) {
	time.Sleep(10 * time.Second)
	response, err := http.Get(url)
	if err != nil {
		log.Fatalf("Couldn't GET image: %v", err)
		return
	}
	defer response.Body.Close()

	if response.StatusCode != 200 {
		log.Fatal("Response code not 200")
	}

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Fatalf("Couldn't read image to bytes: %v", err)
	}

	roomImage[roomKey] = base64.StdEncoding.EncodeToString(body)
	log.Println("Received image")
	s.sendImage(roomKey)
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
