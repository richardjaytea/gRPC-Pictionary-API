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
)

var (
	tls        = flag.Bool("tls", false, "Connection uses TLS if true, else plain TCP")
	certFile   = flag.String("cert_file", "", "The TLS cert file")
	keyFile    = flag.String("key_file", "", "The TLS key file")
	jsonDBFile = flag.String("json_db_file", "", "A json file containing a list of features")
	port       = flag.Int("port", 10002, "The server port")
	emp        = empty.Empty{}
)

type authServer struct {
	pb.UnimplementedAuthServer
}

func (s *authServer) Authenticate(tx context.Context, r *pb.AuthRequest) (*pb.Client, error) {
	id := uuid.NewString()
	return &pb.Client{Id: id, RoomKey: r.RoomKey}, nil
}

func newServer() *authServer {
	s := &authServer{}
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
	pb.RegisterAuthServer(grpcServer, s)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve Chat: %v", err)
	}
}
