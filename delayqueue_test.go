package delayqueue

import (
	"context"
	"log"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestDelayQueue_consume(t *testing.T) {
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
	queue := NewQueue("test", redisCli, UseHashTagKey()).
		WithCallback(cb).
		WithFetchInterval(time.Millisecond * 50).
		WithMaxConsumeDuration(0).
		WithLogger(log.New(os.Stderr, "[DelayQueue]", log.LstdFlags)).
		WithFetchLimit(2)

	for i := 0; i < size; i++ {
		err := queue.SendDelayMsg(strconv.Itoa(i), 0, WithRetryCount(retryCount), WithMsgTTL(time.Hour))
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

func TestDelayQueueOnCluster(t *testing.T) {
	redisCli := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{
			"127.0.0.1:7000",
			"127.0.0.1:7001",
			"127.0.0.1:7002",
		},
	})
	redisCli.FlushDB(context.Background())
	size := 1000
	succeed := 0
	cb := func(s string) bool {
		succeed++
		return true
	}
	queue := NewQueueOnCluster("test", redisCli, cb).
		WithFetchInterval(time.Millisecond * 50).
		WithMaxConsumeDuration(0).
		WithLogger(log.New(os.Stderr, "[DelayQueue]", log.LstdFlags)).
		WithFetchLimit(2).
		WithConcurrent(1)

	for i := 0; i < size; i++ {
		err := queue.SendDelayMsg(strconv.Itoa(i), 0)
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
	if succeed != size {
		t.Error("msg not consumed")
	}
}

func TestDelayQueue_ConcurrentConsume(t *testing.T) {
	redisCli := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	redisCli.FlushDB(context.Background())
	size := 101 // use a prime number may found some hidden bugs ^_^
	retryCount := 3
	mu := sync.Mutex{}
	deliveryCount := make(map[string]int)
	cb := func(s string) bool {
		mu.Lock()
		deliveryCount[s]++
		mu.Unlock()
		return true
	}
	queue := NewQueue("test", redisCli, cb).
		WithFetchInterval(time.Millisecond * 50).
		WithMaxConsumeDuration(0).
		WithLogger(log.New(os.Stderr, "[DelayQueue]", log.LstdFlags)).
		WithConcurrent(4)

	for i := 0; i < size; i++ {
		err := queue.SendDelayMsg(strconv.Itoa(i), 0, WithRetryCount(retryCount), WithMsgTTL(time.Hour))
		if err != nil {
			t.Error(err)
		}
	}
	for i := 0; i < 2*size; i++ {
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
		if v != 1 {
			t.Errorf("expect 1 delivery, actual %d. key: %s", v, k)
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
			t.Log("send stop signal")
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

func TestDelayQueue_AsyncConsume(t *testing.T) {
	size := 10
	redisCli := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	redisCli.FlushDB(context.Background())
	var queue *DelayQueue
	var received int
	queue = NewQueue("exampleAsync", redisCli, func(payload string) bool {
		println(payload)
		received++
		if received == size {
			queue.StopConsume()
			t.Log("send stop signal")
		}
		return true
	}).WithDefaultRetryCount(1)

	// send schedule message
	go func() {
		for {
			time.Sleep(time.Millisecond * 500)
			err := queue.SendScheduleMsg(time.Now().String(), time.Now().Add(time.Second*1))
			if err != nil {
				panic(err)
			}
		}
	}()
	// start consume
	done := queue.StartConsume()
	<-done
}

func TestDelayQueue_Massive_Backlog(t *testing.T) {
	redisCli := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	redisCli.FlushDB(context.Background())
	size := 20000
	retryCount := 3
	cb := func(s string) bool {
		return false
	}
	q := NewQueue("test", redisCli, cb).
		WithFetchInterval(time.Millisecond * 50).
		WithMaxConsumeDuration(0).
		WithLogger(log.New(os.Stderr, "[DelayQueue]", log.LstdFlags)).
		WithFetchLimit(0)

	for i := 0; i < size; i++ {
		err := q.SendDelayMsg(strconv.Itoa(i), 0, WithRetryCount(retryCount))
		if err != nil {
			t.Error(err)
		}
	}
	err := q.pending2Ready()
	if err != nil {
		t.Error(err)
		return
	}
	// consume
	ids := make([]string, 0, q.fetchLimit)
	for {
		idStr, err := q.ready2Unack()
		if err == NilErr { // consumed all
			break
		}
		if err != nil {
			t.Error(err)
			return
		}
		ids = append(ids, idStr)
		if q.fetchLimit > 0 && len(ids) >= int(q.fetchLimit) {
			break
		}
	}
	err = q.unack2Retry()
	if err != nil {
		t.Error(err)
		return
	}
	unackCard, err := redisCli.ZCard(context.Background(), q.unAckKey).Result()
	if err != nil {
		t.Error(err)
		return
	}
	if unackCard != 0 {
		t.Error("unack card should be 0")
		return
	}
	retryLen, err := redisCli.LLen(context.Background(), q.retryKey).Result()
	if err != nil {
		t.Error(err)
		return
	}
	if int(retryLen) != size {
		t.Errorf("unack card should be %d", size)
		return
	}
}


// consume should stopped after actual fetch count hits fetch limit
func TestDelayQueue_FetchLimit(t *testing.T) {
	redisCli := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	redisCli.FlushDB(context.Background())
	fetchLimit := 10
	cb := func(s string) bool {
		return true
	}
	queue := NewQueue("test", redisCli, UseHashTagKey()).
		WithCallback(cb).
		WithFetchInterval(time.Millisecond * 50).
		WithMaxConsumeDuration(0).
		WithLogger(log.New(os.Stderr, "[DelayQueue]", log.LstdFlags)).
		WithFetchLimit(uint(fetchLimit))

	for i := 0; i < fetchLimit; i++ {
		err := queue.SendDelayMsg(strconv.Itoa(i), 0, WithMsgTTL(time.Hour))
		if err != nil {
			t.Error(err)
		}
	}
	// fetch but not consume
	ids1, err := queue.beforeConsume()
	if err != nil {
		t.Errorf("consume error: %v", err)
		return
	}
	// send new messages
	for i := 0; i < fetchLimit; i++ {
		err := queue.SendDelayMsg(strconv.Itoa(i), 0, WithMsgTTL(time.Hour))
		if err != nil {
			t.Error(err)
		}
	}
	ids2, err := queue.beforeConsume()
	if err != nil {
		t.Errorf("consume error: %v", err)
		return
	}
	if len(ids2) > 0 {
		t.Error("should get 0 message, after hitting fetch limit")
	}
	
	// consume
	for _, id := range ids1 {
		queue.callback(id)
	}
	queue.afterConsume()

	// resume
	ids3, err := queue.beforeConsume()
	if err != nil {
		t.Errorf("consume error: %v", err)
		return
	}
	if len(ids3) == 0 {
		t.Error("should get some messages, after consumption")
	}
}

func TestDelayQueue_ScriptPreload(t *testing.T) {
	redisCli := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	redisCli.FlushDB(context.Background())
	size := 101 // use a prime number may found some hidden bugs ^_^
	retryCount := 3
	mu := sync.Mutex{}
	deliveryCount := make(map[string]int)
	cb := func(s string) bool {
		mu.Lock()
		deliveryCount[s]++
		mu.Unlock()
		return true
	}
	queue := NewQueue("test", redisCli, cb).
		WithFetchInterval(time.Millisecond * 50).
		WithMaxConsumeDuration(0).
		WithLogger(log.New(os.Stderr, "[DelayQueue]", log.LstdFlags)).
		WithConcurrent(4).
		WithScriptPreload(true)

	for i := 0; i < size; i++ {
		err := queue.SendDelayMsg(strconv.Itoa(i), 0, WithRetryCount(retryCount), WithMsgTTL(time.Hour))
		if err != nil {
			t.Error(err)
		}
	}
	for i := 0; i < 2*size; i++ {
		if i == 2 {
			// random clean script cache
			redisCli.ScriptFlush(context.Background())
		}
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
		if v != 1 {
			t.Errorf("expect 1 delivery, actual %d. key: %s", v, k)
		}
	}
}