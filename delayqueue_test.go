package delayqueue

import (
	"context"
	"github.com/go-redis/redis/v8"
	"log"
	"math"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestDelayQueue_consume(t *testing.T) {
	redisCli := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	redisCli.FlushDB(context.Background())
	size := 10
	retryCount := 3
	deliveryCount := make(map[string]int)
	cb := func(s string) bool {
		deliveryCount[s]++
		i, _ := strconv.ParseInt(s, 10, 64)
		return i%2 == 0
	}
	queue := NewQueue("test", redisCli, cb).
		WithFetchInterval(time.Millisecond * 50).
		WithMaxConsumeDuration(0).
		WithLogger(log.New(os.Stderr, "[DelayQueue]", log.LstdFlags)).
		WithFetchLimit(1)

	for i := 0; i < size; i++ {
		err := queue.SendDelayMsg(strconv.Itoa(i), 0, WithRetryCount(retryCount))
		if err != nil {
			t.Error(err)
		}
	}
	for i := 0; i < 10*size; i++ {
		err := queue.consume()
		if err != nil {
			t.Errorf("consume error: %v", err)
			return
		}
	}
	for k, v := range deliveryCount {
		i, _ := strconv.ParseInt(k, 10, 64)
		if i%2 == 0 {
			if v != 1 {
				t.Errorf("expect 1 delivery, actual %d", v)
			}
		} else {
			if v != retryCount+1 {
				t.Errorf("expect %d delivery, actual %d", retryCount+1, v)
			}
		}
	}
}

func TestDelayQueue_StopConsume(t *testing.T) {
	size := 10
	redisCli := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	redisCli.FlushDB(context.Background())
	var queue *DelayQueue
	var received int
	queue = NewQueue("test", redisCli, func(s string) bool {
		received++
		if received == size {
			queue.StopConsume()
		}
		return true
	}).WithDefaultRetryCount(1)
	for i := 0; i < size; i++ {
		err := queue.SendDelayMsg(strconv.Itoa(i), 0)
		if err != nil {
			t.Errorf("send message failed: %v", err)
		}
	}
	done := queue.StartConsume()
	<-done
}

func TestIDOverflow(t *testing.T) {
	redisCli := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	redisCli.FlushDB(context.Background())
	queue := NewQueue("test", redisCli, func(s string) bool {
		return true
	})
	err := redisCli.Set(context.Background(), queue.idGenKey, math.MaxInt64, 0).Err()
	if err != nil {
		t.Error(err)
	}
	for i := 0; i < 10; i++ {
		_, err := queue.genId()
		if err != nil {
			t.Error("id gen error")
			return
		}
	}
}
