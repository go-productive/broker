package broker

import (
	"context"
	"github.com/go-redis/redis/v8"
	"log"
	"time"
)

type (
	_RedisStreamPubSubBroker struct {
		options     *_StreamPubSubOptions
		redisClient redis.UniversalClient
		consumer    *Consumer
		stream      string
	}
	_StreamPubSubOptions struct {
		maxLen       int64
		logErrorFunc func(msg string, keysAndValues ...interface{})
	}
	StreamPubSubOption func(*_StreamPubSubOptions)
)

func newStreamPubSubOptions(opts ...StreamPubSubOption) *_StreamPubSubOptions {
	o := &_StreamPubSubOptions{
		maxLen: 1_0000,
		logErrorFunc: func(msg string, keysAndValues ...interface{}) {
			log.Println(append([]interface{}{"msg", msg}, keysAndValues...)...)
		},
	}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

func WithPubSubMaxLen(maxLen int64) StreamPubSubOption {
	return func(o *_StreamPubSubOptions) {
		o.maxLen = maxLen
	}
}

func WithPubSubLogErrorFunc(logErrorFunc func(msg string, keysAndValues ...interface{})) StreamPubSubOption {
	return func(o *_StreamPubSubOptions) {
		o.logErrorFunc = logErrorFunc
	}
}

func NewRedisStreamPubSubBroker(redisClient redis.UniversalClient, stream string, consumer *Consumer, opts ...StreamPubSubOption) *_RedisStreamPubSubBroker {
	r := &_RedisStreamPubSubBroker{
		options:     newStreamPubSubOptions(opts...),
		redisClient: redisClient,
		consumer:    consumer,
		stream:      stream,
	}
	if consumer.ConsumeFunc != nil {
		go r.loopConsume()
	}
	return r
}

func (r *_RedisStreamPubSubBroker) loopConsume() {
	lastID := "$"
	for {
		xStreams, err := r.redisClient.XRead(context.TODO(), &redis.XReadArgs{
			Streams: []string{r.stream, lastID},
			Count:   batchSize,
			Block:   0,
		}).Result()
		if err == redis.ErrClosed {
			return
		}
		if err != nil {
			r.options.logErrorFunc("_RedisStreamPubSubBroker.loopConsume", "err", err, "stream", r.stream)
			time.Sleep(time.Second)
			continue
		}
		for _, xStream := range xStreams {
			for _, xMessage := range xStream.Messages {
				bs := []byte(xMessage.Values["bs"].(string))
				if err := r.consumer.ConsumeFunc(bs); err != nil {
					r.options.logErrorFunc("_RedisStreamPubSubBroker.loopConsume", "err", err, "stream", r.stream, "bs", bs)
				}
			}
			lastID = xStream.Messages[len(xStream.Messages)-1].ID
		}
	}
}

func (r *_RedisStreamPubSubBroker) Pub(message interface{}) error {
	bs, err := r.consumer.MarshalFunc(message)
	if err != nil {
		return err
	}
	return r.redisClient.XAdd(context.TODO(), &redis.XAddArgs{
		Stream: r.stream,
		MaxLen: r.options.maxLen,
		Approx: true,
		Values: map[string]interface{}{
			"bs": string(bs),
		},
	}).Err()
}
