package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net"
	"time"

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
	roomImage map[string]image
	roomWord  map[string][]string
)

type imageStreams []*pb.Image_GetImageServer

type imageServer struct {
	pb.UnimplementedImageServer
	roomImageStreams map[string]imageStreams
	roomWordStreams  map[string]*pb.Image_GetWordServer
	DB               *sql.DB
}

type image struct {
	Id  string `json:"photo_id"`
	Url string `json:"photo_image_url"`
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

func (s *imageServer) sendKeywords(roomKey string) {
	time.Sleep(5 * time.Second)
	s.sendWord(roomKey, "@SysClearWords")
	for _, k := range roomWord[roomKey] {
		s.sendWord(roomKey, k)
	}
}

func (s *imageServer) sendWord(roomKey string, word string) {
	stream := *s.roomWordStreams[roomKey]
	if err := stream.Send(&pb.WordResponse{
		Word: word,
	}); err != nil {
		log.Println(err)
	}
}

func (s *imageServer) sendImage(roomKey string) {
	time.Sleep(5 * time.Second)
	for _, stream := range s.roomImageStreams[roomKey] {
		if stream != nil && *stream != nil {
			if err := (*stream).Send(&pb.ImageResponse{
				Content: roomImage[roomKey].Url,
			}); err != nil {
				log.Println(err)
			}
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
		roomImageStreams: make(map[string]imageStreams),
		roomWordStreams:  make(map[string]*pb.Image_GetWordServer),
		DB:               db,
	}

	return s
}

func initVars() {
	roomImage = make(map[string]image)
	roomWord = make(map[string][]string)
}

func (s *imageServer) getRandomImage(roomKey string) {
	var i image
	stmt := "SELECT photo_id, photo_image_url FROM unsplash_photos ORDER BY random() LIMIT 1"
	s.DB.QueryRow(stmt).Scan(&i.Id, &i.Url)

	log.Println(i)
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
			order by
				ai_service_1_confidence desc`

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
			go s.sendImage(v)
			go s.sendKeywords(v)
		}
	}
}

func (s *imageServer) startCron() {
	c := cron.New()
	c.AddFunc("@every 30s", s.refreshImageAndSendFunc())
	c.Start()
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
	initVars()
	pb.RegisterImageServer(grpcServer, s)

	s.startCron()

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve Chat: %v", err)
	}
}
