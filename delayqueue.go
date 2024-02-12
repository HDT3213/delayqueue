package delayqueue

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	"log"
	"strconv"
	"sync"
	"time"
)

// DelayQueue is a message queue supporting delayed/scheduled delivery based on redis
type DelayQueue struct {
	// name for this Queue. Make sure the name is unique in redis database
	name          string
	redisCli      RedisCli
	cb            func(string) bool
	pendingKey    string // sorted set: message id -> delivery time
	readyKey      string // list
	unAckKey      string // sorted set: message id -> retry time
	retryKey      string // list
	retryCountKey string // hash: message id -> remain retry count
	garbageKey    string // set: message id
	useHashTag    bool
	ticker        *time.Ticker
	logger        *log.Logger
	close         chan struct{}

	maxConsumeDuration time.Duration
	msgTTL             time.Duration
	defaultRetryCount  uint
	fetchInterval      time.Duration
	fetchLimit         uint
	concurrent         uint
}

// NilErr represents redis nil
var NilErr = errors.New("nil")

// RedisCli is abstraction for redis client, required commands only not all commands
type RedisCli interface {
	Eval(script string, keys []string, args []interface{}) (interface{}, error) // args should be string, integer or float
	Set(key string, value string, expiration time.Duration) error
	Get(key string) (string, error)
	Del(keys []string) error
	HSet(key string, field string, value string) error
	HDel(key string, fields []string) error
	SMembers(key string) ([]string, error)
	SRem(key string, members []string) error
	ZAdd(key string, values map[string]float64) error
	ZRem(key string, fields []string) error
}

type hashTagKeyOpt int

// UseHashTagKey add hashtags to redis keys to ensure all keys of this queue are allocated in the same hash slot.
// If you are using Codis/AliyunRedisCluster/TencentCloudRedisCluster, add this option to NewQueue
// WARNING! Changing (add or remove) this option will cause DelayQueue failing to read existed data in redis
// see more:  https://redis.io/docs/reference/cluster-spec/#hash-tags
func UseHashTagKey() interface{} {
	return hashTagKeyOpt(1)
}

// NewQueue0 creates a new queue, use DelayQueue.StartConsume to consume or DelayQueue.SendScheduleMsg to publish message
// callback returns true to confirm successful consumption. If callback returns false or not return within maxConsumeDuration, DelayQueue will re-deliver this message
func NewQueue0(name string, cli RedisCli, callback func(string) bool, opts ...interface{}) *DelayQueue {
	if name == "" {
		panic("name is required")
	}
	if cli == nil {
		panic("cli is required")
	}
	if callback == nil {
		panic("callback is required")
	}
	useHashTag := false
	for _, opt := range opts {
		switch opt.(type) {
		case hashTagKeyOpt:
			useHashTag = true
		}
	}
	var keyPrefix string
	if useHashTag {
		keyPrefix = "{dp:" + name + "}"
	} else {
		keyPrefix = "dp:" + name
	}
	return &DelayQueue{
		name:               name,
		redisCli:           cli,
		cb:                 callback,
		pendingKey:         keyPrefix + ":pending",
		readyKey:           keyPrefix + ":ready",
		unAckKey:           keyPrefix + ":unack",
		retryKey:           keyPrefix + ":retry",
		retryCountKey:      keyPrefix + ":retry:cnt",
		garbageKey:         keyPrefix + ":garbage",
		useHashTag:         useHashTag,
		close:              make(chan struct{}, 1),
		maxConsumeDuration: 5 * time.Second,
		msgTTL:             time.Hour,
		logger:             log.Default(),
		defaultRetryCount:  3,
		fetchInterval:      time.Second,
		concurrent:         1,
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

// WithFetchLimit limits the max number of unack (processing) messages
func (q *DelayQueue) WithFetchLimit(limit uint) *DelayQueue {
	q.fetchLimit = limit
	return q
}

// WithConcurrent sets the number of concurrent consumers
func (q *DelayQueue) WithConcurrent(c uint) *DelayQueue {
	if c == 0 {
		return q
	}
	q.concurrent = c
	return q
}

// WithDefaultRetryCount customizes the max number of retry, it effects of messages in this queue
// use WithRetryCount during DelayQueue.SendScheduleMsg or DelayQueue.SendDelayMsg to specific retry count of particular message
func (q *DelayQueue) WithDefaultRetryCount(count uint) *DelayQueue {
	q.defaultRetryCount = count
	return q
}

func (q *DelayQueue) genMsgKey(idStr string) string {
	if q.useHashTag {
		return "{dp:" + q.name + "}" + ":msg:" + idStr
	}
	return "dp:" + q.name + ":msg:" + idStr
}

type retryCountOpt int

// WithRetryCount set retry count for a msg
// example: queue.SendDelayMsg(payload, duration, delayqueue.WithRetryCount(3))
func WithRetryCount(count int) interface{} {
	return retryCountOpt(count)
}

type msgTTLOpt time.Duration

// WithMsgTTL set ttl for a msg
// example: queue.SendDelayMsg(payload, duration, delayqueue.WithMsgTTL(Hour))
func WithMsgTTL(d time.Duration) interface{} {
	return msgTTLOpt(d)
}

// SendScheduleMsg submits a message delivered at given time
func (q *DelayQueue) SendScheduleMsg(payload string, t time.Time, opts ...interface{}) error {
	// parse options
	retryCount := q.defaultRetryCount
	for _, opt := range opts {
		switch o := opt.(type) {
		case retryCountOpt:
			retryCount = uint(o)
		case msgTTLOpt:
			q.msgTTL = time.Duration(o)
		}
	}
	// generate id
	idStr := uuid.Must(uuid.NewRandom()).String()
	now := time.Now()
	// store msg
	msgTTL := t.Sub(now) + q.msgTTL // delivery + q.msgTTL
	err := q.redisCli.Set(q.genMsgKey(idStr), payload, msgTTL)
	if err != nil {
		return fmt.Errorf("store msg failed: %v", err)
	}
	// store retry count
	err = q.redisCli.HSet(q.retryCountKey, idStr, strconv.Itoa(int(retryCount)))
	if err != nil {
		return fmt.Errorf("store retry count failed: %v", err)
	}
	// put to pending
	err = q.redisCli.ZAdd(q.pendingKey, map[string]float64{idStr: float64(t.Unix())})
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
// keys: pendingKey, readyKey
// argv: currentTime
const pending2ReadyScript = `
local msgs = redis.call('ZRangeByScore', KEYS[1], '0', ARGV[1])  -- get ready msg
if (#msgs == 0) then return end
local args2 = {} -- keys to push into ready
for _,v in ipairs(msgs) do
	table.insert(args2, v) 
    if (#args2 == 4000) then
		redis.call('LPush', KEYS[2], unpack(args2))
		args2 = {}
	end
end
if (#args2 > 2) then 
	redis.call('LPush', KEYS[2], unpack(args2))
end
redis.call('ZRemRangeByScore', KEYS[1], '0', ARGV[1])  -- remove msgs from pending
`

func (q *DelayQueue) pending2Ready() error {
	now := time.Now().Unix()
	keys := []string{q.pendingKey, q.readyKey}
	_, err := q.redisCli.Eval(pending2ReadyScript, keys, []interface{}{now})
	if err != nil && err != NilErr {
		return fmt.Errorf("pending2ReadyScript failed: %v", err)
	}
	return nil
}

// ready2UnackScript atomically moves messages from ready to unack
// keys: readyKey/retryKey, unackKey
// argv: retryTime
const ready2UnackScript = `
local msg = redis.call('RPop', KEYS[1])
if (not msg) then return end
redis.call('ZAdd', KEYS[2], ARGV[1], msg)
return msg
`

func (q *DelayQueue) ready2Unack() (string, error) {
	retryTime := time.Now().Add(q.maxConsumeDuration).Unix()
	keys := []string{q.readyKey, q.unAckKey}
	ret, err := q.redisCli.Eval(ready2UnackScript, keys, []interface{}{retryTime})
	if err == NilErr {
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
	keys := []string{q.retryKey, q.unAckKey}
	ret, err := q.redisCli.Eval(ready2UnackScript, keys, []interface{}{retryTime, q.retryKey, q.unAckKey})
	if err == NilErr {
		return "", NilErr
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

func (q *DelayQueue) callback(idStr string) error {
	payload, err := q.redisCli.Get(q.genMsgKey(idStr))
	if err == NilErr {
		return nil
	}
	if err != nil {
		// Is an IO error?
		return fmt.Errorf("get message payload failed: %v", err)
	}
	ack := q.cb(payload)
	if ack {
		err = q.ack(idStr)
	} else {
		err = q.nack(idStr)
	}
	return err
}

// batchCallback calls DelayQueue.callback in batch. callback is executed concurrently according to property DelayQueue.concurrent
// batchCallback must wait all callback finished, otherwise the actual number of processing messages may beyond DelayQueue.FetchLimit
func (q *DelayQueue) batchCallback(ids []string) {
	if len(ids) == 1 || q.concurrent == 1 {
		for _, id := range ids {
			err := q.callback(id)
			if err != nil {
				q.logger.Printf("consume msg %s failed: %v", id, err)
			}
		}
		return
	}
	ch := make(chan string, len(ids))
	for _, id := range ids {
		ch <- id
	}
	close(ch)
	wg := sync.WaitGroup{}
	concurrent := int(q.concurrent)
	if concurrent > len(ids) { // too many goroutines is no use
		concurrent = len(ids)
	}
	wg.Add(concurrent)
	for i := 0; i < concurrent; i++ {
		go func() {
			defer wg.Done()
			for id := range ch {
				err := q.callback(id)
				if err != nil {
					q.logger.Printf("consume msg %s failed: %v", id, err)
				}
			}
		}()
	}
	wg.Wait()
}

func (q *DelayQueue) ack(idStr string) error {
	err := q.redisCli.ZRem(q.unAckKey, []string{idStr})
	if err != nil {
		return fmt.Errorf("remove from unack failed: %v", err)
	}
	// msg key has ttl, ignore result of delete
	_ = q.redisCli.Del([]string{q.genMsgKey(idStr)})
	_ = q.redisCli.HDel(q.retryCountKey, []string{idStr})
	return nil
}

func (q *DelayQueue) nack(idStr string) error {
	// update retry time as now, unack2Retry will move it to retry immediately
	err := q.redisCli.ZAdd(q.unAckKey, map[string]float64{
		idStr: float64(time.Now().Unix()),
	})
	if err != nil {
		return fmt.Errorf("negative ack failed: %v", err)
	}
	return nil
}

// unack2RetryScript atomically moves messages from unack to retry which remaining retry count greater than 0,
// and moves messages from unack to garbage which  retry count is 0
// Because DelayQueue cannot determine garbage message before eval unack2RetryScript, so it cannot pass keys parameter to redisCli.Eval
// Therefore unack2RetryScript moves garbage message to garbageKey instead of deleting directly
// keys: unackKey, retryCountKey, retryKey, garbageKey
// argv: currentTime
const unack2RetryScript = `
local unack2retry = function(msgs)
	local retryCounts = redis.call('HMGet', KEYS[2], unpack(msgs)) -- get retry count
	for i,v in ipairs(retryCounts) do
		local k = msgs[i]
		if v ~= false and v ~= nil and v ~= '' and tonumber(v) > 0 then
			redis.call("HIncrBy", KEYS[2], k, -1) -- reduce retry count
			redis.call("LPush", KEYS[3], k) -- add to retry
		else
			redis.call("HDel", KEYS[2], k) -- del retry count
			redis.call("SAdd", KEYS[4], k) -- add to garbage
		end
	end
end

local msgs = redis.call('ZRangeByScore', KEYS[1], '0', ARGV[1])  -- get retry msg
if (#msgs == 0) then return end
if #msgs < 4000 then
	unack2retry(msgs)
else
	local buf = {}
	for _,v in ipairs(msgs) do
		table.insert(buf, v)
		if #buf == 4000 then
			unack2retry(buf)
			buf = {}
		end
	end
	if (#buf > 0) then
		unack2retry(buf)
	end
end
redis.call('ZRemRangeByScore', KEYS[1], '0', ARGV[1])  -- remove msgs from unack
`

func (q *DelayQueue) unack2Retry() error {
	keys := []string{q.unAckKey, q.retryCountKey, q.retryKey, q.garbageKey}
	now := time.Now()
	_, err := q.redisCli.Eval(unack2RetryScript, keys, []interface{}{now.Unix()})
	if err != nil && err != NilErr {
		return fmt.Errorf("unack to retry script failed: %v", err)
	}
	return nil
}

func (q *DelayQueue) garbageCollect() error {
	msgIds, err := q.redisCli.SMembers(q.garbageKey)
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
	err = q.redisCli.Del(msgKeys)
	if err != nil && err != NilErr {
		return fmt.Errorf("del msgs failed: %v", err)
	}
	err = q.redisCli.SRem(q.garbageKey, msgIds)
	if err != nil && err != NilErr {
		return fmt.Errorf("remove from garbage key failed: %v", err)
	}
	return nil
}

func (q *DelayQueue) consume() error {
	// pending to ready
	err := q.pending2Ready()
	if err != nil {
		return err
	}
	// consume
	ids := make([]string, 0, q.fetchLimit)
	for {
		idStr, err := q.ready2Unack()
		if err == NilErr { // consumed all
			break
		}
		if err != nil {
			return err
		}
		ids = append(ids, idStr)
		if q.fetchLimit > 0 && len(ids) >= int(q.fetchLimit) {
			break
		}
	}
	if len(ids) > 0 {
		q.batchCallback(ids)
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
	ids = make([]string, 0, q.fetchLimit)
	for {
		idStr, err := q.retry2Unack()
		if err == NilErr { // consumed all
			break
		}
		if err != nil {
			return err
		}
		ids = append(ids, idStr)
		if q.fetchLimit > 0 && len(ids) >= int(q.fetchLimit) {
			break
		}
	}
	if len(ids) > 0 {
		q.batchCallback(ids)
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
		close(done0)
	}()
	return done0
}

// StopConsume stops consumer goroutine
func (q *DelayQueue) StopConsume() {
	close(q.close)
	if q.ticker != nil {
		q.ticker.Stop()
	}
}
