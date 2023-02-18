package redis

import (
	"context"
	"fmt"
	"wrapper/internal/logger"
	"wrapper/proto"
	"strings"
	"time"

	"github.com/go-redis/redis/v9"
)

// configuration variables hardcoded for convenience
var (
	Stream              = "pb-stream"
	ConsGroup           = "pb-delivery"
	ConsumerID          = 1
	MsgQueryCount int64 = 100 // arbitrary - default for some redis queries
	redisAddr           = "redis:6379"
	redisPW             = ""
	redisDB             = 0
)

// RedisConsumer implements Queue interface
type RedisConsumer struct {
	client     *redis.Client
	stream     string
	group      string
	consumer   string
	queryCount int64
	log        *logger.MyLogger
}

// Calls redisMsgChan to instantiate a new channel of *redis.XMessage
// Pulls messages from that channel, translates each message, and pushes them
// onto the return channel for processing
func (r *RedisConsumer) NewMessageChannel(ctx context.Context, cd *proto.ConsumerData) <-chan *proto.Postback {
	r.applyConsumerData(cd)
	ch := make(chan *proto.Postback)
	rch := r.redisMsgChan(ctx)

	go func() {
		defer close(ch)
		for rm := range rch {
			m := redisMsgToMessage(r.log, rm)
			ch <- m
		}
	}()

	return ch
}

func (r *RedisConsumer) AckMessage(id string) error {
	ack := r.client.XAck(context.Background(), r.stream, r.group, id)
	_, err := ack.Result()
	// r.log.Info(fmt.Sprintf("message acked to redis: %s, err: %v", id, err)) // noisy
	return err
}

// instantiates a new Redis client to read from stream using embedded config variables
func New(l *logger.MyLogger) *RedisConsumer { // Add logger to instantiate
	return &RedisConsumer{
		client: redis.NewClient(&redis.Options{
			Addr:     redisAddr,
			Password: redisPW,
			DB:       redisDB,
		}),
		queryCount: int64(MsgQueryCount),
		log:        l,
	}
}

// rawMsgChan continually queries the stream and pushes each redis message
// to a buffered channel.
func (r *RedisConsumer) redisMsgChan(ctx context.Context) <-chan *redis.XMessage {
	mchan := make(chan *redis.XMessage, r.queryCount)
	claim, err := r.staleMsgs(ctx)
	if err != nil {
		r.log.Error(fmt.Sprintf("failed to claim pending messages: %v", err))
	}
	r.createGroup(ctx)

	go func() {
		defer close(mchan)
		for {
			select {

			case stale := <-claim:
				mchan <- stale

			case <-ctx.Done():
				r.log.Info("context cancelled -> finished reading Redis stream")
				return

			default:
				messages, err := r.readStream(ctx) // blocks for
				if err != nil {
					// handle error
					r.log.Error(fmt.Sprintf("failed to read stream: %v", err))
				}
				for i := range messages {
					mchan <- &messages[i]
				}
			}
		}
	}()

	return mchan
}

// embeds stream, group, consumer info from ConsumerData request object and embeds
// info into RedisConsumer instance for querying stream on behalf of requesting client worker
func (r *RedisConsumer) applyConsumerData(cd *proto.ConsumerData) {
	r.stream = cd.GetStreamKey()
	r.group = cd.GetGroupName()
	r.consumer = cd.GetConsumerID()
}

// parses redis stream message into protobuf message
// no validation explicitly performed by this function (see parseMsgVal)
func redisMsgToMessage(l *logger.MyLogger, m *redis.XMessage) *proto.Postback {
	method := parseMsgVal(l, m, "method")
	url := parseMsgVal(l, m, "url")

	return &proto.Postback{
		Method: method,
		Url:    url,
		AckID:  m.ID,
	}
}

// Parses entry in stream message values map corresponding with the given key.
// A value that can't be parsed as a string is processed as the empty string
func parseMsgVal(l *logger.MyLogger, m *redis.XMessage, key string) string {
	iface, ok := m.Values[key]
	if !ok {
		iface = ""
	}

	// val initialized to "" if type assertion fails
	val, _ := iface.(string)
	return val
}

// Performs single query of stream for the Consumer Group (the application) and Consumer (application instance)
// using Redis's `XReadGroup` command. Returns `r.QueryCount` messages at a time
// (Docs are unclear if XReadGroup is safe for concurrent use.)
func (r *RedisConsumer) readStream(ctx context.Context) ([]redis.XMessage, error) {
	queryArgs := r.xReadGrArgs(">") // ">" special ID to query undelivered messages
	query := r.client.XReadGroup(ctx, queryArgs)

	streams, err := query.Result()
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	for _, stream := range streams {
		return stream.Messages, nil
	}

	return nil, nil
}

// creates consumer group in Redis instance on behalf of the caller
func (r *RedisConsumer) createGroup(ctx context.Context) {
	rCmd := r.client.XGroupCreate(context.Background(), r.stream, r.group, "0")
	_, err := rCmd.Result()
	if err!= nil && strings.Split(err.Error(), " ")[0] != "BUSYGROUP" {
		// non-trivial group create error
		r.log.Error("failed creating group", err.Error())
	}
}

// retrieves stale unacked messages for the consumer group and returns
// them as a channel
func (r *RedisConsumer) staleMsgs(ctx context.Context) (<-chan *redis.XMessage, error) {
	var claimed = make(chan *redis.XMessage)

	go func() {
		defer close(claimed)
		for {
			msgs, err := r.claimPending(ctx)
			if err != nil {
				r.log.Error("error from claimPending", err.Error())
			}
			for i := range msgs {
				claimed <- &msgs[i]
			}
			time.Sleep(2 * time.Minute)
		}
	}()
	return claimed, nil
}

func (r *RedisConsumer) claimPending(ctx context.Context) ([]redis.XMessage, error) {
	var (
		args         = r.xAutoClaimArgs()
		query        = r.client.XAutoClaim(ctx, args)
		msgs, _, err = query.Result()
	)
	if err != nil {
		r.log.Error(err.Error())
	}
	return msgs, err
}

// prebuilds arguments for XReadGroup query, starting after the given message ID in the stream
func (r *RedisConsumer) xReadGrArgs(queryStartID string) *redis.XReadGroupArgs {
	return &redis.XReadGroupArgs{
		Group:    r.group,
		Consumer: r.consumer,
		Streams:  []string{r.stream, queryStartID},
		Count:    r.queryCount,
		Block:    10000, // how long readStream will block
		NoAck:    false,
	}
}

// prebuilds arguments for XAutoClaim query
func (r *RedisConsumer) xAutoClaimArgs() *redis.XAutoClaimArgs {
	return &redis.XAutoClaimArgs{
		Stream:   r.stream,
		Group:    r.group,
		MinIdle:  60 * time.Second, // arbitrary staleness threshold
		Start:    "0-0",
		Count:    MsgQueryCount,
		Consumer: r.consumer,
	}
}
