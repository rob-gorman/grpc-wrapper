package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"wrapper/cmd/api"
	"wrapper/internal/logger"
	"wrapper/internal/queue/redis"
	"wrapper/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var port int = 50051 // recommended port

func main() {
	l := logger.New()
	queue := redis.New(l)
	api := api.New(queue, l)

	ctx, cancel := signal.NotifyContext(
		context.Background(),
		syscall.SIGTERM,
		syscall.SIGQUIT,
		os.Interrupt,
	)

	// grpc.NumStreamWorkers(uint32) might be useful Server Option here
	gs := grpc.NewServer()
	reflection.Register(gs) // useful for grpcurl

	// register api as gRPC service
	proto.RegisterPostbackStreamServer(gs, api)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		l.Error("failed to listen on PORT", fmt.Sprint(port))
	}

	l.Info("gRPC service available on PORT", fmt.Sprint(port))

	go func() {
		defer cancel()
		<-ctx.Done()
		l.Info("shutdown request recieved - stopping server")
		gs.GracefulStop() // blocking
		l.Info("graceful shutdown successful")
	}()

	gs.Serve(lis)
}
