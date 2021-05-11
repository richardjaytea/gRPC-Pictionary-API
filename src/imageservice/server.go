package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net"

	"github.com/golang/protobuf/ptypes/empty"
	c "github.com/richardjaytea/infipic/config"
	"github.com/richardjaytea/infipic/pb"
	"github.com/robfig/cron/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/examples/data"

	_ "github.com/lib/pq"
)

var (
	tls                = flag.Bool("tls", false, "Connection uses TLS if true, else plain TCP")
	certFile           = flag.String("cert_file", "", "The TLS cert file")
	keyFile            = flag.String("key_file", "", "The TLS key file")
	jsonDBFile         = flag.String("json_db_file", "", "A json file containing a list of features")
	port               = flag.Int("port", 10001, "The server port")
	emp                = empty.Empty{}
	caFile             = flag.String("ca_file", "", "The file containing the CA root cert file")
	serverAddrRoom     = flag.String("server_addr_room", "localhost:10003", "The server address for the room service server")
	serverHostOverride = flag.String("server_host_override", "x.test.youtube.com", "The server name used to verify the hostname returned by the TLS handshake")
)

var (
	rooms     []string
	roomImage map[string]image
	roomWord  map[string][]string
)

type imageWordStreams map[string]*pb.Image_GetImageAndWordsServer

type imageServer struct {
	pb.UnimplementedImageServer
	roomImageWordStreams map[string]imageWordStreams
	roomClient           pb.RoomClient
	DB                   *sql.DB
}

type image struct {
	Id  string `json:"photo_id"`
	Url string `json:"photo_image_url"`
}

func (s *imageServer) GetImageAndWords(r *pb.Client, stream pb.Image_GetImageAndWordsServer) error {
	s.roomImageWordStreams[r.RoomKey][r.Id] = &stream
	s.sendImageToUser(r.RoomKey, r.Id)
	log.Printf("ImageWord Stream Created: %s %s", r.RoomKey, r.Id)
	select {
	case <-stream.Context().Done():
		// TODO: Lock map for deletion?
		delete(s.roomImageWordStreams[r.RoomKey], r.Id)
		log.Printf("ImageWord Connection Disconnected: %s %s", r.RoomKey, r.Id)
		return nil
	}
}

func (s *imageServer) sendImageAndWords(roomKey string) {
	for _, stream := range s.roomImageWordStreams[roomKey] {
		if stream != nil && *stream != nil {
			if err := (*stream).Send(&pb.ImageWordResponse{
				Content: roomImage[roomKey].Url,
				Words:   roomWord[roomKey],
			}); err != nil {
				log.Println(err)
			}
		}
	}
}

func (s *imageServer) sendImageToUser(roomKey string, id string) {
	if stream, ok := s.roomImageWordStreams[roomKey][id]; ok {
		if err := (*stream).Send(&pb.ImageWordResponse{
			Content: roomImage[roomKey].Url,
			Words:   roomWord[roomKey],
		}); err != nil {
			log.Println(err)
		}
	}
}

func newServer() *imageServer {
	connectionString := fmt.Sprintf("host=%s port=%s user=%s "+
		"password=%s dbname=%s sslmode=disable",
		c.VGetEnv("APP_DB_HOST"),
		c.VGetEnv("APP_DB_PORT"),
		c.VGetEnv("APP_DB_USERNAME"),
		c.VGetEnv("APP_DB_PASSWORD"),
		c.VGetEnv("APP_DB_NAME"))

	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		panic(err)
	}

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	log.Println("Connected to database!")

	s := &imageServer{
		roomImageWordStreams: make(map[string]imageWordStreams),
		DB:                   db,
	}
	roomImage = make(map[string]image)
	roomWord = make(map[string][]string)

	s.connectServices()

	for _, v := range rooms {
		s.roomImageWordStreams[v] = imageWordStreams{}
		roomImage[v] = image{}
		roomWord[v] = []string{}
	}

	return s
}

func (s *imageServer) getRooms() {
	r, err := s.roomClient.GetRooms(context.Background(), &empty.Empty{})
	if err != nil {
		log.Fatal("error in trying to get rooms from room service")
		return
	}

	for _, v := range r.Rooms {
		rooms = append(rooms, v.GetKey())
	}
}

func (s *imageServer) getRandomImage(roomKey string) {
	var i image
	stmt := "SELECT photo_id, photo_image_url FROM unsplash_photos ORDER BY random() LIMIT 1"
	s.DB.QueryRow(stmt).Scan(&i.Id, &i.Url)

	roomImage[roomKey] = i
}

func (s *imageServer) getImageKeywords(roomKey string) {
	stmt := `select
				keyword
			from
				unsplash_keywords uk
			where
				photo_id = $1
				and ((ai_service_1_confidence is not null
				and ai_service_1_confidence > 40.0)
				or (ai_service_2_confidence is not null
				and ai_service_2_confidence > 40.0))
				and suggested_by_user = false
				and keyword not like '% %'
				and keyword not like '%-%'
			order by
				ai_service_1_confidence desc
			limit 6`

	id := roomImage[roomKey].Id
	r, err := s.DB.Query(stmt, id)
	if err != nil && err != sql.ErrNoRows {
		log.Fatalf("Error trying to get keywords for ID: %s", id)
		return
	}

	var k []string
	var v string

	defer r.Close()
	for r.Next() {
		r.Scan(&v)
		k = append(k, v)
	}

	roomWord[roomKey] = k
	log.Println(roomWord[roomKey])
}

func (s *imageServer) refreshImageAndSendFunc() func() {
	return func() {
		for _, v := range rooms {
			s.getRandomImage(v)
			s.getImageKeywords(v)
			go s.sendImageAndWords(v)
		}
	}
}

func (s *imageServer) startCron() {
	c := cron.New()
	c.AddFunc("@every 30s", s.refreshImageAndSendFunc())
	c.Start()
}

func (s *imageServer) connectServices() {
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
	s := newServer()
	pb.RegisterImageServer(grpcServer, s)

	s.startCron()

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve Chat: %v", err)
	}
}
