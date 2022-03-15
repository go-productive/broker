package broker

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"sync/atomic"
	"time"
)

const (
	batchSize = 100
)

type (
	MQBroker interface {
		Put(message interface{}) error
		WaitConsumeFinish()
	}
	PubSubBroker interface {
		Pub(message interface{}) error
	}
	Consumer struct {
		MarshalFunc func(interface{}) ([]byte, error)
		// if not nil, consume background in other goroutine
		ConsumeFunc func(bs []byte) error
	}
)

func mongoObjectID() string {
	var bs [12]byte
	binary.BigEndian.PutUint32(bs[0:4], uint32(time.Now().Unix()))
	copy(bs[4:9], processUnique[:])
	putUint24(bs[9:12], atomic.AddUint32(&objectIDCounter, 1))
	return hex.EncodeToString(bs[:])
}

var (
	objectIDCounter = readRandomUint32()
	processUnique   = processUniqueBytes()
)

func processUniqueBytes() [5]byte {
	var b [5]byte
	if _, err := io.ReadFull(rand.Reader, b[:]); err != nil {
		panic(fmt.Errorf("cannot initialize objectid package with crypto.rand.Reader: %v", err))
	}
	return b
}

func readRandomUint32() uint32 {
	var b [4]byte
	if _, err := io.ReadFull(rand.Reader, b[:]); err != nil {
		panic(fmt.Errorf("cannot initialize objectid package with crypto.rand.Reader: %v", err))
	}
	return (uint32(b[0]) << 0) | (uint32(b[1]) << 8) | (uint32(b[2]) << 16) | (uint32(b[3]) << 24)
}

func putUint24(b []byte, v uint32) {
	b[0] = byte(v >> 16)
	b[1] = byte(v >> 8)
	b[2] = byte(v)
}
