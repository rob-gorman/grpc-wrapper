package api

import (
	"context"
	"os/signal"
	"runtime"
	"syscall"
	"time"
	"wrapper/internal/logger"
	"wrapper/internal/queue"
	"wrapper/proto"
)

var (
	workers = runtime.NumCPU() // arbitrary starting point
	ctx, _  = signal.NotifyContext(
		context.Background(),
		syscall.SIGTERM,
		syscall.SIGINT,
		syscall.SIGQUIT,
	)
)

// implements protobuf PostbackStreamServer interface
type QueueAPI struct {
	queue queue.Queue
	log   *logger.MyLogger
	proto.UnimplementedPostbackStreamServer
}

// Subscribe initializes a channel via the caller's queue.NewMessageChannel method and
// streams the messages from that channel to the client
func (api *QueueAPI) Subscribe(cd *proto.ConsumerData, rs proto.PostbackStream_SubscribeServer) error {
	stream := api.queue.NewMessageChannel(ctx, cd)
	var err error
	var sem = make(chan struct{}, workers)

	for postback := range stream {
		sem <- struct{}{} // bounded concurrency

		go func(postback *proto.Postback) {
			defer func() { <-sem }()
			err1 := rs.Send(postback)
			if err1 != nil {
				// client unavailable? close connection
				err = err1
				api.log.Error("failed to send message ID", postback.AckID)
				return
			}
		}(postback)

		if err != nil {
			break
		}
	}
	return err
}

// AckPostback recieves client ack for a postback and passes it to queue implementation
func (api *QueueAPI) AckPostback(as proto.PostbackStream_AckPostbackServer) error {
	var sem = make(chan struct{}, workers)
	var err error

	for {
		sem <- struct{}{}

		go func() {
			defer func() { <-sem }()
			ack, err1 := as.Recv()
			if ack == nil { // debug
				time.Sleep(2 * time.Second)
				return
			}
			if err1 != nil {
				err = err1 // this is a race condition, but we don't really care
				api.log.Error("grpc conn failed to recieve Ack message")
			}
			err = api.queue.AckMessage(ack.MessageID)
			as.Send(&proto.Empty{})
		}()

		if err != nil {
			return err
		}
	}
}

func New(q queue.Queue, l *logger.MyLogger) *QueueAPI {
	return &QueueAPI{
		queue: q,
		log:   l,
	}
}
