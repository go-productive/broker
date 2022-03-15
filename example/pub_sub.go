package main

import (
	"fmt"
	"github.com/go-productive/broker"
	"github.com/go-redis/redis/v8"
	"time"
)

func main() {
	redisClient := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{
			"192.168.42.141:34300",
			"192.168.42.141:34301",
			"192.168.42.141:34302",
			"192.168.42.141:34303",
			"192.168.42.141:34304",
			"192.168.42.141:34305",
		},
	})
	newConsumer := func(name string) *broker.Consumer {
		return &broker.Consumer{
			MarshalFunc: func(req interface{}) ([]byte, error) {
				return []byte(req.(string)), nil
			},
			ConsumeFunc: func(bs []byte) error {
				fmt.Println(name, string(bs))
				return nil
			},
		}
	}

	pubSub := broker.NewRedisStreamPubSubBroker(redisClient, "test_stream_pub_sub", newConsumer("consumer1"))
	broker.NewRedisStreamPubSubBroker(redisClient, "test_stream_pub_sub", newConsumer("consumer2"))
	for time.Sleep(time.Millisecond); ; time.Sleep(time.Second * 5) {
		if err := pubSub.Pub(time.Now().String()); err != nil {
			panic(err)
		}
	}
}
