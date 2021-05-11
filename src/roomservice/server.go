package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"github.com/golang/protobuf/ptypes/empty"
	c "github.com/richardjaytea/infipic/config"
	"github.com/richardjaytea/infipic/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/examples/data"
	"log"
	"net"

	_ "github.com/lib/pq"
)

var (
	tls        = flag.Bool("tls", false, "Connection uses TLS if true, else plain TCP")
	certFile   = flag.String("cert_file", "", "The TLS cert file")
	keyFile    = flag.String("key_file", "", "The TLS key file")
	jsonDBFile = flag.String("json_db_file", "", "A json file containing a list of features")
	port       = flag.Int("port", 10003, "The server port")
)

type roomServer struct {
	pb.UnimplementedRoomServer
	DB *sql.DB
}

type room struct {
	Name string `json:"name"`
	Key  string `json:"key"`
}

func (s *roomServer) GetRooms(ctx context.Context, e *empty.Empty) (*pb.RoomResponse, error) {
	r, err := s.GetAllRooms()
	if err != nil {
		return nil, err
	}

	return &pb.RoomResponse{
		Rooms: r,
	}, nil
}

func (s *roomServer) GetAllRooms() ([]*pb.RoomDetail, error) {
	var r *pb.RoomDetail
	var a []*pb.RoomDetail

	stmt := "SELECT name, key FROM room ORDER BY name"
	result, err := s.DB.Query(stmt)
	if err != nil && err != sql.ErrNoRows {
		log.Fatalln("Error trying to get rooms")
		return nil, err
	}

	defer result.Close()
	for result.Next() {
		r = new(pb.RoomDetail)
		result.Scan(&r.Name, &r.Key)
		a = append(a, r)
	}

	return a, nil
}

func newServer() *roomServer {
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

	s := &roomServer{
		DB: db,
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
	s := newServer()
	pb.RegisterRoomServer(grpcServer, s)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve Chat: %v", err)
	}
}
