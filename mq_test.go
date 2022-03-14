package broker

import (
	"testing"
	"time"
)

func TestMQ(t *testing.T) {
	opts := []StreamMQOption{
		WithMsgIdleTime(time.Second * 10),
		WithConsumerIdleTime(time.Minute),
	}
	mqBroker, err := NewRedisStreamMQBroker(redisClient, "test_stream_mq", newConsumer("consumer1"), opts...)
	if err != nil {
		panic(err)
	}
	//_, err = NewRedisStreamMQBroker(redisClient, "test_stream_mq", newConsumer("consumer2"), opts...)
	//if err != nil {
	//	panic(err)
	//}
	//_, err = NewRedisStreamMQBroker(redisClient, "test_stream_mq", newConsumer("consumer3"), opts...)
	//if err != nil {
	//	panic(err)
	//}

	for i := 0; i < 1; i++ {
		_panic(mqBroker.Put(time.Now().String()))
	}
	select {}
}
