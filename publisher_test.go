package delayqueue

import (
	"context"
	"log"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestPublisher(t *testing.T) {
	redisCli := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	redisCli.FlushDB(context.Background())
	size := 1000
	retryCount := 3
	deliveryCount := make(map[string]int)
	cb := func(s string) bool {
		deliveryCount[s]++
		i, _ := strconv.ParseInt(s, 10, 64)
		return i%2 == 0
	}
	logger := log.New(os.Stderr, "[DelayQueue]", log.LstdFlags)
	queue := NewQueue("test", redisCli, cb).WithLogger(logger)
	publisher := NewPublisher("test", redisCli).WithLogger(logger)

	for i := 0; i < size; i++ {
		err := publisher.SendDelayMsg(strconv.Itoa(i), 0, WithRetryCount(retryCount), WithMsgTTL(time.Hour))
		if err != nil {
			t.Error(err)
		}
	}
	for i := 0; i < 10*size; i++ {
		ids, err := queue.beforeConsume()
		if err != nil {
			t.Errorf("consume error: %v", err)
			return
		}
		for _, id := range ids {
			queue.callback(id)
		}
		queue.afterConsume()
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
