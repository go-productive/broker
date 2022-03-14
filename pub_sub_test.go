package broker

import (
	"fmt"
	"github.com/go-redis/redis/v8"
	"testing"
	"time"
)

var (
	redisClient = redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{
			"192.168.42.141:34300",
			"192.168.42.141:34301",
			"192.168.42.141:34302",
			"192.168.42.141:34303",
			"192.168.42.141:34304",
			"192.168.42.141:34305",
		},
	})
)

func newConsumer(name string) *Consumer {
	return &Consumer{
		MarshalFunc: func(req interface{}) ([]byte, error) {
			return []byte(req.(string)), nil
		},
		ConsumeFunc: func(bs []byte) error {
			fmt.Println(name, string(bs))
			return nil
		},
	}
}

func TestPubSub(t *testing.T) {
	broker := NewRedisStreamPubSubBroker(redisClient, "test_stream_pub_sub", newConsumer("consumer1"))
	NewRedisStreamPubSubBroker(redisClient, "test_stream_pub_sub", newConsumer("consumer2"))
	time.Sleep(time.Millisecond)
	_panic(broker.Pub(time.Now().String()))
	_panic(broker.Pub(time.Now().String()))
	_panic(broker.Pub(time.Now().String()))
	select {}
}

func _panic(err error) {
	if err != nil {
		panic(err)
	}
}
