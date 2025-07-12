package delayqueue

import (
	"context"
	"log"
	"os"
	"strconv"
	"strings"
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
	queue.garbageCollect()
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
		WithConcurrent(4).
		WithScriptPreload(false)

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

func TestDelayQueue_NackRedeliveryDelay(t *testing.T) {
	redisCli := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	redisCli.FlushDB(context.Background())
	cb := func(s string) bool {
		return false
	}
	redeliveryDelay := time.Second
	queue := NewQueue("test", redisCli, UseHashTagKey()).
		WithCallback(cb).
		WithFetchInterval(time.Millisecond * 50).
		WithLogger(log.New(os.Stderr, "[DelayQueue]", log.LstdFlags)).
		WithDefaultRetryCount(3).
		WithNackRedeliveryDelay(redeliveryDelay)

	err := queue.SendScheduleMsg("foobar", time.Now().Add(-time.Minute))
	if err != nil {
		t.Error(err)
	}
	// first consume, callback will failed
	ids, err := queue.beforeConsume()
	if err != nil {
		t.Errorf("consume error: %v", err)
		return
	}
	for _, id := range ids {
		queue.callback(id)
	}
	queue.afterConsume()

	// retry immediately
	ids, err = queue.beforeConsume()
	if err != nil {
		t.Errorf("consume error: %v", err)
		return
	}
	if len(ids) != 0 {
		t.Errorf("should not redeliver immediately")
		return
	}

	time.Sleep(redeliveryDelay)
	queue.afterConsume()
	ids, err = queue.beforeConsume()
	if err != nil {
		t.Errorf("consume error: %v", err)
		return
	}
	if len(ids) != 1 {
		t.Errorf("should not redeliver immediately")
		return
	}
}

// TestDelayQueue_Issue16_RestartScenario reproduces and verifies the fix for Issue #16
func TestDelayQueue_Issue16_RestartScenario(t *testing.T) {
	redisCli := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	redisCli.FlushDB(context.Background())
	
	t.Log("=== Issue #16: nackRedeliveryDelay not respected after restart ===")
	
	// Test scenario: message is nacked, then service restarts before
	// nackRedeliveryDelay expires, causing immediate consumption
	
	nackRedeliveryDelay := 3 * time.Second
	maxConsumeDuration := 2 * time.Second
	
	consumeCount := 0
	cb := func(s string) bool {
		consumeCount++
		t.Logf("Message consumed #%d at %v", consumeCount, time.Now())
		return false // nack
	}
	
	queue1 := NewQueue("issue16-test", redisCli, UseHashTagKey()).
		WithCallback(cb).
		WithMaxConsumeDuration(maxConsumeDuration).
		WithDefaultRetryCount(3).
		WithNackRedeliveryDelay(nackRedeliveryDelay)
	
	// Send and nack message
	err := queue1.SendScheduleMsg("issue16-msg", time.Now())
	if err != nil {
		t.Error(err)
		return
	}
	
	// Consume and nack
	nackTime := time.Now()
	ids, err := queue1.beforeConsume()
	if err != nil {
		t.Error(err)
		return
	}
	for _, id := range ids {
		queue1.callback(id)
	}
	queue1.afterConsume()
	
	t.Logf("Message nacked at %v, should not be available until %v", 
		nackTime, nackTime.Add(nackRedeliveryDelay))
	
	// Force message to retry queue by waiting for nackRedeliveryDelay
	t.Logf("Waiting for nackRedeliveryDelay (%v) to expire...", nackRedeliveryDelay)
	time.Sleep(nackRedeliveryDelay + 100*time.Millisecond)
	
	err = queue1.unack2Retry()
	if err != nil {
		t.Error(err)
		return
	}
	
	// Verify message is now in retry queue (this is the key state for the bug)
	ctx := context.Background()
	retryCount, _ := redisCli.LLen(ctx, queue1.retryKey).Result()
	if retryCount != 1 {
		t.Fatalf("Expected 1 message in retry queue, got %d", retryCount)
	}
	t.Log("✅ Message successfully moved to retry queue")
	
	// CRITICAL: Simulate restart by creating new queue instance
	// The bug was that retry2Unack() didn't respect nackRedeliveryDelay
	restartTime := time.Now()
	consumeCount2 := 0
	cb2 := func(s string) bool {
		consumeCount2++
		consumeTime := time.Now()
		timeSinceRestart := consumeTime.Sub(restartTime)
		actualDelay := consumeTime.Sub(nackTime)
		
		t.Logf("RESTART: Message consumed #%d at %v", consumeCount2, consumeTime)
		t.Logf("RESTART: %.2fs after restart, %.2fs after original nack", 
			timeSinceRestart.Seconds(), actualDelay.Seconds())
		
		// With the fix, message should wait for nackRedeliveryDelay from restart time
		expectedMinDelay := nackRedeliveryDelay
		if timeSinceRestart < expectedMinDelay {
			t.Errorf("❌ Message consumed too early: %.2fs after restart, should wait at least %.2fs", 
				timeSinceRestart.Seconds(), expectedMinDelay.Seconds())
		} else {
			t.Logf("✅ Fix verified: Message waited %.2fs after restart", timeSinceRestart.Seconds())
		}
		
		return true
	}
	
	queue2 := NewQueue("issue16-test", redisCli, UseHashTagKey()).
		WithCallback(cb2).
		WithMaxConsumeDuration(maxConsumeDuration).
		WithDefaultRetryCount(3).
		WithNackRedeliveryDelay(nackRedeliveryDelay)
	
	t.Logf("RESTART: New queue instance created at %v", restartTime)
	
	// Try to consume immediately after restart
	ids, err = queue2.beforeConsume()
	if err != nil {
		t.Error(err)
		return
	}
	
	t.Logf("RESTART: beforeConsume() returned %d messages immediately", len(ids))
	
	if len(ids) > 0 {
		// This would be the bug - immediate consumption
		for _, id := range ids {
			queue2.callback(id)
		}
		queue2.afterConsume()
	} else {
		// If no immediate consumption, wait for proper delay
		t.Log("No immediate consumption, waiting for nackRedeliveryDelay...")
		time.Sleep(nackRedeliveryDelay + 100*time.Millisecond)
		
		ids, err = queue2.beforeConsume()
		if err != nil {
			t.Error(err)
			return
		}
		
		if len(ids) > 0 {
			for _, id := range ids {
				queue2.callback(id)
			}
			queue2.afterConsume()
		}
	}
	
	if consumeCount2 == 0 {
		t.Error("Message was never consumed by the second queue")
	}
}

func TestDelayQueue_TryIntercept(t *testing.T) {
	redisCli := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	redisCli.FlushDB(context.Background())
	cb := func(s string) bool {
		return false
	}
	queue := NewQueue("test", redisCli, cb).
		WithDefaultRetryCount(3).
		WithNackRedeliveryDelay(time.Minute)

	// intercept pending message
	msg, err := queue.SendDelayMsgV2("foobar", time.Minute)
	if err != nil {
		t.Error(err)
		return
	}
	result, err := queue.TryIntercept(msg)
	if err != nil {
		t.Error(err)
		return
	}
	if !result.Intercepted {
		t.Error("expect intercepted")
	}

	// intercept ready message
	msg, err = queue.SendScheduleMsgV2("foobar2", time.Now().Add(-time.Minute))
	if err != nil {
		t.Error(err)
		return
	}
	err = queue.pending2Ready()
	if err != nil {
		t.Error(err)
		return
	}
	result, err = queue.TryIntercept(msg)
	if err != nil {
		t.Error(err)
		return
	}
	if !result.Intercepted {
		t.Error("expect intercepted")
	}

	// prevent from retry
	msg, err = queue.SendScheduleMsgV2("foobar3", time.Now().Add(-time.Minute))
	if err != nil {
		t.Error(err)
		return
	}
	ids, err := queue.beforeConsume()
	if err != nil {
		t.Errorf("consume error: %v", err)
		return
	}
	for _, id := range ids {
		queue.nack(id)
	}
	queue.afterConsume()
	result, err = queue.TryIntercept(msg)
	if err != nil {
		t.Error(err)
		return
	}
	if result.Intercepted {
		t.Error("expect not intercepted")
		return
	}
	ids, err = queue.beforeConsume()
	if err != nil {
		t.Errorf("consume error: %v", err)
		return
	}
	if len(ids) > 0 {
		t.Error("expect empty messages")
	}
}

func TestUseCustomPrefix(t *testing.T) {
	redisCli := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	cb := func(s string) bool {
		return false
	}
	prefix := "MYQUEUE"
	dp := NewQueue("test", redisCli, cb, UseCustomPrefix(prefix))
	if !strings.HasPrefix(dp.pendingKey, prefix) {
		t.Error("wrong prefix")
	}
	if !strings.HasPrefix(dp.readyKey, prefix) {
		t.Error("wrong prefix")
	}
	if !strings.HasPrefix(dp.unAckKey, prefix) {
		t.Error("wrong prefix")
	}
	if !strings.HasPrefix(dp.retryKey, prefix) {
		t.Error("wrong prefix")
	}
	if !strings.HasPrefix(dp.retryCountKey, prefix) {
		t.Error("wrong prefix")
	}
	if !strings.HasPrefix(dp.garbageKey, prefix) {
		t.Error("wrong prefix")
	}
}