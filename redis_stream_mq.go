package broker

import (
	"context"
	"github.com/go-redis/redis/v8"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	redisBusyGroupErr = "BUSYGROUP Consumer Group name already exists"
	defaultGroup      = "default"
)

type (
	_RedisStreamMQBroker struct {
		options     *_StreamMQOptions
		redisClient redis.UniversalClient
		consumer    *Consumer
		stream      string
		consumerID  string
		waitGroup   sync.WaitGroup
	}
	XInfoConsumer struct {
		Name    string
		Pending int64
		Idle    time.Duration
	}
	_StreamMQOptions struct {
		maxLen           int64
		msgIdleTime      time.Duration
		consumerIdleTime time.Duration
		consumerCount    int
		idFunc           func(message interface{}) string
		logErrorFunc     func(msg string, keysAndValues ...interface{})
	}
	StreamMQOption func(*_StreamMQOptions)
)

func newStreamMQOptions(opts ...StreamMQOption) *_StreamMQOptions {
	s := &_StreamMQOptions{
		msgIdleTime:      time.Minute * 5,
		consumerIdleTime: time.Hour * 24,
		consumerCount:    1,
		idFunc: func(message interface{}) string {
			return ""
		},
		logErrorFunc: func(msg string, keysAndValues ...interface{}) {
			log.Println(append([]interface{}{"msg", msg}, keysAndValues...)...)
		},
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func WithMQMaxLen(maxLen int64) StreamMQOption {
	return func(o *_StreamMQOptions) {
		o.maxLen = maxLen
	}
}

func WithMsgIdleTime(msgIdleTime time.Duration) StreamMQOption {
	return func(o *_StreamMQOptions) {
		o.msgIdleTime = msgIdleTime
	}
}

func WithConsumerIdleTime(consumerIdleTime time.Duration) StreamMQOption {
	return func(o *_StreamMQOptions) {
		o.consumerIdleTime = consumerIdleTime
	}
}

func WithConsumerCount(consumerCount int) StreamMQOption {
	return func(o *_StreamMQOptions) {
		o.consumerCount = consumerCount
	}
}

func WithIDFunc(idFunc func(message interface{}) string) StreamMQOption {
	return func(o *_StreamMQOptions) {
		o.idFunc = idFunc
	}
}

func WithMQLogErrorFunc(logErrorFunc func(msg string, keysAndValues ...interface{})) StreamMQOption {
	return func(o *_StreamMQOptions) {
		o.logErrorFunc = logErrorFunc
	}
}

func NewRedisStreamMQBroker(redisClient redis.UniversalClient, stream string, consumer *Consumer, opts ...StreamMQOption) (*_RedisStreamMQBroker, error) {
	r := &_RedisStreamMQBroker{
		options:     newStreamMQOptions(opts...),
		redisClient: redisClient,
		consumer:    consumer,
		stream:      stream,
		consumerID:  mongoObjectID(),
	}
	if consumer.ConsumeFunc != nil {
		if err := r.redisClient.XGroupCreateMkStream(context.TODO(), stream, defaultGroup, "$").Err(); err != nil && !strings.Contains(err.Error(), redisBusyGroupErr) {
			return nil, err
		}
		if err := r.timerToEvictConsumer(); err != nil {
			return nil, err
		}
		go r.timerToHandlePending()
		for i := 0; i < r.options.consumerCount; i++ {
			r.waitGroup.Add(1)
			go r.loopConsume()
		}
	}
	return r, nil
}

func (r *_RedisStreamMQBroker) timerToHandlePending() {
	for range time.Tick(r.options.msgIdleTime >> 1) {
		if err := r.handlePending(); err != nil {
			r.options.logErrorFunc("timerToHandlePending", "err", err, "stream", r.stream)
		}
	}
}

func (r *_RedisStreamMQBroker) handlePending() error {
	for {
		xPendingExtList, err := r.redisClient.XPendingExt(context.TODO(), &redis.XPendingExtArgs{
			Stream: r.stream,
			Group:  defaultGroup,
			Start:  "-",
			End:    strconv.FormatInt(time.Now().Add(-r.options.msgIdleTime).UnixNano()/int64(time.Millisecond), 10),
			Count:  batchSize,
		}).Result()
		if err != nil {
			return err
		}
		if len(xPendingExtList) <= 0 {
			return nil
		}
		messages := make([]string, 0, len(xPendingExtList))
		for _, xPendingExt := range xPendingExtList {
			messages = append(messages, xPendingExt.ID)
		}
		xMessages, err := r.redisClient.XClaim(context.TODO(), &redis.XClaimArgs{
			Stream:   r.stream,
			Group:    defaultGroup,
			Consumer: r.consumerID,
			MinIdle:  r.options.msgIdleTime,
			Messages: messages,
		}).Result()
		if err != nil {
			return err
		}
		r.consumeAndAck(xMessages)
	}
}

func (r *_RedisStreamMQBroker) timerToEvictConsumer() error {
	if err := r.evictConsumer(); err != nil {
		return err
	}
	go func() {
		for range time.Tick(r.options.consumerIdleTime) {
			if err := r.evictConsumer(); err != nil {
				r.options.logErrorFunc("timerToEvictConsumer", "err", err, "stream", r.stream)
			}
		}
	}()
	return nil
}

func (r *_RedisStreamMQBroker) evictConsumer() error {
	consumers, err := r.redisClient.XInfoConsumers(context.TODO(), r.stream, defaultGroup).Result()
	if err != nil {
		return err
	}
	_, err = r.redisClient.Pipelined(context.TODO(), func(pipeline redis.Pipeliner) error {
		for _, consumer := range consumers {
			if time.Duration(consumer.Idle)*time.Millisecond > r.options.consumerIdleTime {
				pipeline.XGroupDelConsumer(context.TODO(), r.stream, defaultGroup, consumer.Name)
			}
		}
		return nil
	})
	return err
}

func (r *_RedisStreamMQBroker) loopConsume() {
	defer r.waitGroup.Done()
	for {
		xStreams, err := r.redisClient.XReadGroup(context.TODO(), &redis.XReadGroupArgs{
			Group:    defaultGroup,
			Consumer: r.consumerID,
			Streams:  []string{r.stream, ">"},
			Count:    batchSize,
			Block:    0,
		}).Result()
		if err == redis.ErrClosed {
			return
		}
		if err != nil {
			r.options.logErrorFunc("loopConsume", "err", err, "stream", r.stream)
			time.Sleep(time.Second)
			continue
		}
		for _, xStream := range xStreams {
			r.consumeAndAck(xStream.Messages)
		}
	}
}

func (r *_RedisStreamMQBroker) consumeAndAck(xMessages []redis.XMessage) {
	_, err := r.redisClient.Pipelined(context.TODO(), func(pipeline redis.Pipeliner) error {
		ids := make([]string, 0, len(xMessages))
		for _, xMessage := range xMessages {
			bs := []byte(xMessage.Values["bs"].(string))
			if err := r.consumer.ConsumeFunc(bs); err == nil {
				ids = append(ids, xMessage.ID)
			}
		}
		if len(ids) > 0 {
			pipeline.XAck(context.TODO(), r.stream, defaultGroup, ids...)
			pipeline.XDel(context.TODO(), r.stream, ids...)
		}
		return nil
	})
	if err != nil {
		r.options.logErrorFunc("consumeAndAck", "err", err, "stream", r.stream)
	}
}

func (r *_RedisStreamMQBroker) Put(message interface{}) error {
	bs, err := r.consumer.MarshalFunc(message)
	if err != nil {
		return err
	}
	return r.redisClient.XAdd(context.TODO(), &redis.XAddArgs{
		Stream: r.stream,
		MaxLen: r.options.maxLen,
		Approx: true,
		ID:     r.options.idFunc(message),
		Values: map[string]interface{}{
			"bs": string(bs),
		},
	}).Err()
}

func (r *_RedisStreamMQBroker) WaitConsumeFinish() {
	r.waitGroup.Wait()
}
