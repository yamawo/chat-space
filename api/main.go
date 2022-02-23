package main

import (
	"chat-space/api/pb"
	"chat-space/api/server"
	"log"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	log.Println("starting server")
	lis, err := net.Listen("tcp", "0.0.0.0:50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()
	chat.RegisterChatServer(s, &server.Server{})

	reflection.Register(s)

	client := server.NewRedisClient()
	log.Print(client.Ping().Err())

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
