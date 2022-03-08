package delayqueue

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"log"
	"math"
	"strconv"
	"time"
)

// DelayQueue is a message queue supporting delayed/scheduled delivery based on redis
type DelayQueue struct {
	// name for this Queue. Make sure the name is unique in redis database
	name          string
	redisCli      *redis.Client
	cb            func(string) bool
	pendingKey    string // sorted set: message id -> delivery time
	readyKey      string // list
	unAckKey      string // sorted set: message id -> retry time
	retryKey      string // list
	retryCountKey string // hash: message id -> remain retry count
	garbageKey    string // set: message id
	idGenKey      string // id generator
	ticker        *time.Ticker
	logger        *log.Logger
	close         chan struct{}

	maxConsumeDuration time.Duration
	msgTTL             time.Duration
	defaultRetryCount  int
	fetchInterval      time.Duration
	fetchLimit         uint
}

// NewQueue creates a new queue, use DelayQueue.StartConsume to consume or DelayQueue.SendScheduleMsg to publish message
// callback returns true to confirm successful consumption. If callback returns false or not return within maxConsumeDuration, DelayQueue will re-deliver this message
func NewQueue(name string, cli *redis.Client, callback func(string) bool) *DelayQueue {
	if name == "" {
		panic("name is required")
	}
	if cli == nil {
		panic("cli is required")
	}
	if callback == nil {
		panic("callback is required")
	}
	return &DelayQueue{
		name:               name,
		redisCli:           cli,
		cb:                 callback,
		pendingKey:         "dp:" + name + ":pending",
		readyKey:           "dp:" + name + ":ready",
		unAckKey:           "dp:" + name + ":unack",
		retryKey:           "dp:" + name + ":retry",
		retryCountKey:      "dp:" + name + ":retry:cnt",
		garbageKey:         "dp:" + name + ":garbage",
		idGenKey:           "dp:" + name + ":id_gen",
		close:              make(chan struct{}, 1),
		maxConsumeDuration: 5 * time.Second,
		msgTTL:             time.Hour,
		logger:             log.Default(),
		defaultRetryCount:  3,
		fetchInterval:      time.Second,
		fetchLimit:         math.MaxInt32,
	}
}

// WithLogger customizes logger for queue
func (q *DelayQueue) WithLogger(logger *log.Logger) *DelayQueue {
	q.logger = logger
	return q
}

// WithFetchInterval customizes the interval at which consumer fetch message from redis
func (q *DelayQueue) WithFetchInterval(d time.Duration) *DelayQueue {
	q.fetchInterval = d
	return q
}

// WithMaxConsumeDuration customizes max consume duration
// If no acknowledge received within WithMaxConsumeDuration after message delivery, DelayQueue will try to deliver this message again
func (q *DelayQueue) WithMaxConsumeDuration(d time.Duration) *DelayQueue {
	q.maxConsumeDuration = d
	return q
}

// WithFetchLimit limits the max number of messages at one time
func (q *DelayQueue) WithFetchLimit(limit uint) *DelayQueue {
	q.fetchLimit = limit
	return q
}

func (q *DelayQueue) genMsgKey(idStr string) string {
	return "dp:" + q.name + ":msg:" + idStr
}

func (q *DelayQueue) genId() (uint32, error) {
	ctx := context.Background()
	id, err := q.redisCli.Incr(ctx, q.idGenKey).Result()
	if err != nil && err.Error() == "ERR increment or decrement would overflow" {
		err = q.redisCli.Set(ctx, q.idGenKey, 1, 0).Err()
		if err != nil {
			return 0, fmt.Errorf("reset id gen failed: %v", err)
		}
		return 1, nil
	}
	if err != nil {
		return 0, fmt.Errorf("incr id gen failed: %v", err)
	}
	return uint32(id), nil
}

type retryCountOpt int

// WithRetryCount set retry count for a msg
// example: queue.SendDelayMsg(payload, duration, delayqueue.WithRetryCount(3))
func WithRetryCount(count int) interface{} {
	return retryCountOpt(count)
}

// SendScheduleMsg submits a message delivered at given time
func (q *DelayQueue) SendScheduleMsg(payload string, t time.Time, opts ...interface{}) error {
	// parse options
	retryCount := q.defaultRetryCount
	for _, opt := range opts {
		switch o := opt.(type) {
		case retryCountOpt:
			retryCount = int(o)
		}
	}
	// generate id
	id, err := q.genId()
	if err != nil {
		return err
	}
	idStr := strconv.FormatUint(uint64(id), 10)
	ctx := context.Background()
	now := time.Now()
	// store msg
	msgTTL := t.Sub(now) + q.msgTTL // delivery + q.msgTTL
	err = q.redisCli.Set(ctx, q.genMsgKey(idStr), payload, msgTTL).Err()
	if err != nil {
		return fmt.Errorf("store msg failed: %v", err)
	}
	// store retry count
	err = q.redisCli.HSet(ctx, q.retryCountKey, idStr, retryCount).Err()
	if err != nil {
		return fmt.Errorf("store retry count failed: %v", err)
	}
	// put to pending
	err = q.redisCli.ZAdd(ctx, q.pendingKey, &redis.Z{Score: float64(t.Unix()), Member: id}).Err()
	if err != nil {
		return fmt.Errorf("push to pending failed: %v", err)
	}
	return nil
}

// SendDelayMsg submits a message delivered after given duration
func (q *DelayQueue) SendDelayMsg(payload string, duration time.Duration, opts ...interface{}) error {
	t := time.Now().Add(duration)
	return q.SendScheduleMsg(payload, t, opts...)
}

// pending2ReadyScript atomically moves messages from pending to ready
// argv: currentTime, pendingKey, readyKey
const pending2ReadyScript = `
local msgs = redis.call('ZRangeByScore', ARGV[2], '0', ARGV[1])  -- get ready msg
if (#msgs == 0) then return end
local args2 = {'LPush', ARGV[3]} -- push into ready
for _,v in ipairs(msgs) do
	table.insert(args2, v) 
end
redis.call(unpack(args2))
redis.call('ZRemRangeByScore', ARGV[2], '0', ARGV[1])  -- remove msgs from pending
`

func (q *DelayQueue) pending2Ready() error {
	now := time.Now().Unix()
	ctx := context.Background()
	keys := []string{q.pendingKey, q.readyKey}
	err := q.redisCli.Eval(ctx, pending2ReadyScript, keys, now, q.pendingKey, q.readyKey).Err()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("pending2ReadyScript failed: %v", err)
	}
	return nil
}

// pending2ReadyScript atomically moves messages from ready to unack
// argv: retryTime, readyKey/retryKey, unackKey
const ready2UnackScript = `
local msg = redis.call('RPop', ARGV[2])
if (not msg) then return end
redis.call('ZAdd', ARGV[3], ARGV[1], msg)
return msg
`

func (q *DelayQueue) ready2Unack() (string, error) {
	retryTime := time.Now().Add(q.maxConsumeDuration).Unix()
	ctx := context.Background()
	keys := []string{q.readyKey, q.unAckKey}
	ret, err := q.redisCli.Eval(ctx, ready2UnackScript, keys, retryTime, q.readyKey, q.unAckKey).Result()
	if err == redis.Nil {
		return "", err
	}
	if err != nil {
		return "", fmt.Errorf("ready2UnackScript failed: %v", err)
	}
	str, ok := ret.(string)
	if !ok {
		return "", fmt.Errorf("illegal result: %#v", ret)
	}
	return str, nil
}

func (q *DelayQueue) retry2Unack() (string, error) {
	retryTime := time.Now().Add(q.maxConsumeDuration).Unix()
	ctx := context.Background()
	keys := []string{q.retryKey, q.unAckKey}
	ret, err := q.redisCli.Eval(ctx, ready2UnackScript, keys, retryTime, q.retryKey, q.unAckKey).Result()
	if err == redis.Nil {
		return "", redis.Nil
	}
	if err != nil {
		return "", fmt.Errorf("ready2UnackScript failed: %v", err)
	}
	str, ok := ret.(string)
	if !ok {
		return "", fmt.Errorf("illegal result: %#v", ret)
	}
	return str, nil
}

func (q *DelayQueue) callback(idStr string) (bool, error) {
	ctx := context.Background()
	payload, err := q.redisCli.Get(ctx, q.genMsgKey(idStr)).Result()
	if err == redis.Nil {
		return true, nil
	}
	if err != nil {
		// Is an IO error?
		return false, fmt.Errorf("get message payload failed: %v", err)
	}
	return q.cb(payload), nil
}

func (q *DelayQueue) ack(idStr string) error {
	ctx := context.Background()
	err := q.redisCli.ZRem(ctx, q.unAckKey, idStr).Err()
	if err != nil {
		return fmt.Errorf("remove from unack failed: %v", err)
	}
	// msg key has ttl, ignore result of delete
	_ = q.redisCli.Del(ctx, q.genMsgKey(idStr)).Err()
	q.redisCli.HDel(ctx, q.retryCountKey, idStr)
	return nil
}

// unack2RetryScript atomically moves messages from unack to retry which remaining retry count greater than 0,
// and moves messages from unack to garbage which  retry count is 0
// Because DelayQueue cannot determine garbage message before eval unack2RetryScript, so it cannot pass keys parameter to redisCli.Eval
// Therefore unack2RetryScript moves garbage message to garbageKey instead of deleting directly
// argv: currentTime, unackKey, retryCountKey, retryKey, garbageKey
const unack2RetryScript = `
local msgs = redis.call('ZRangeByScore', ARGV[2], '0', ARGV[1])  -- get retry msg
if (#msgs == 0) then return end
local retryCounts = redis.call('HMGet', ARGV[3], unpack(msgs)) -- get retry count
for i,v in ipairs(retryCounts) do
	local k = msgs[i]
	if tonumber(v) > 0 then
		redis.call("HIncrBy", ARGV[3], k, -1) -- reduce retry count
		redis.call("LPush", ARGV[4], k) -- add to retry
	else
		redis.call("HDel", ARGV[3], k) -- del retry count
		redis.call("SAdd", ARGV[5], k) -- add to garbage
	end
end
redis.call('ZRemRangeByScore', ARGV[2], '0', ARGV[1])  -- remove msgs from unack
`

func (q *DelayQueue) unack2Retry() error {
	ctx := context.Background()
	keys := []string{q.unAckKey, q.retryKey, q.retryCountKey, q.garbageKey}
	now := time.Now()
	err := q.redisCli.Eval(ctx, unack2RetryScript, keys,
		now.Unix(), q.unAckKey, q.retryCountKey, q.retryKey, q.garbageKey).Err()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("unack to retry script failed: %v", err)
	}
	return nil
}

func (q *DelayQueue) garbageCollect() error {
	ctx := context.Background()
	msgIds, err := q.redisCli.SMembers(ctx, q.garbageKey).Result()
	if err != nil {
		return fmt.Errorf("smembers failed: %v", err)
	}
	if len(msgIds) == 0 {
		return nil
	}
	// allow concurrent clean
	msgKeys := make([]string, 0, len(msgIds))
	for _, idStr := range msgIds {
		msgKeys = append(msgKeys, q.genMsgKey(idStr))
	}
	err = q.redisCli.Del(ctx, msgKeys...).Err()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("del msgs failed: %v", err)
	}
	err = q.redisCli.SRem(ctx, q.garbageKey, msgIds).Err()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("remove from garbage key failed: %v", err)
	}
	return nil
}

func (q *DelayQueue) consume() error {
	// pending2ready
	err := q.pending2Ready()
	if err != nil {
		return err
	}
	// consume
	var fetchCount uint
	for {
		idStr, err := q.ready2Unack()
		if err == redis.Nil { // consumed all
			break
		}
		if err != nil {
			return err
		}
		fetchCount++
		ack, err := q.callback(idStr)
		if err != nil {
			return err
		}
		if ack {
			err = q.ack(idStr)
			if err != nil {
				return err
			}
		}
		if fetchCount >= q.fetchLimit {
			break
		}
	}
	// unack to retry
	err = q.unack2Retry()
	if err != nil {
		return err
	}
	err = q.garbageCollect()
	if err != nil {
		return err
	}
	// retry
	fetchCount = 0
	for {
		idStr, err := q.retry2Unack()
		if err == redis.Nil { // consumed all
			break
		}
		if err != nil {
			return err
		}
		fetchCount++
		ack, err := q.callback(idStr)
		if err != nil {
			return err
		}
		if ack {
			err = q.ack(idStr)
			if err != nil {
				return err
			}
		}
		if fetchCount >= q.fetchLimit {
			break
		}
	}
	return nil
}

// StartConsume creates a goroutine to consume message from DelayQueue
// use `<-done` to wait consumer stopping
func (q *DelayQueue) StartConsume() (done <-chan struct{}) {
	q.ticker = time.NewTicker(q.fetchInterval)
	done0 := make(chan struct{})
	go func() {
	tickerLoop:
		for {
			select {
			case <-q.ticker.C:
				err := q.consume()
				if err != nil {
					log.Printf("consume error: %v", err)
				}
			case <-q.close:
				break tickerLoop
			}
		}
		done0 <- struct{}{}
	}()
	return done0
}

// StopConsume stops consumer goroutine
func (q *DelayQueue) StopConsume() {
	q.close <- struct{}{}
	if q.ticker != nil {
		q.ticker.Stop()
	}
}
