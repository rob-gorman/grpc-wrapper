# gRPC API flow

## Init QueueAPI

init grpcServerStream
init redisClient
init AckServer & PostbackServer

api.ReadPostbackStream(in, api.pbs) error {
  init stream data (consumer group)
}