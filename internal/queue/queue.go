package queue

import (
	"context"
	"wrapper/proto"
)

// Queue is the interface abstraction for our message queue implementation
type Queue interface {
	NewMessageChannel(ctx context.Context, cd *proto.ConsumerData) <-chan *proto.Postback
	AckMessage(id string) error // this should actually be type interface{}
}
