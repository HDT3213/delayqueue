package delayqueue

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// DelayQueue is a message queue supporting delayed/scheduled delivery based on redis
type DelayQueue struct {
	// name for this Queue. Make sure the name is unique in redis database
	name               string
	redisCli           RedisCli
	cb                 func(string) bool
	pendingKey         string // sorted set: message id -> delivery time
	readyKey           string // list
	unAckKey           string // sorted set: message id -> retry time
	retryKey           string // list
	retryCountKey      string // hash: message id -> remain retry count
	garbageKey         string // set: message id
	useHashTag         bool
	ticker             *time.Ticker
	logger             Logger
	close              chan struct{}
	running            int32
	maxConsumeDuration time.Duration // default 5 seconds
	msgTTL             time.Duration // default 1 hour
	defaultRetryCount  uint          // default 3
	fetchInterval      time.Duration // default 1 second
	fetchLimit         uint          // default no limit
	fetchCount         int32         // actually running task number
	concurrent         uint          // default 1, executed serially
	sha1map            map[string]string
	sha1mapMu          *sync.RWMutex
	scriptPreload      bool
	// for batch consume
	consumeBuffer chan string

	eventListener       EventListener
	nackRedeliveryDelay time.Duration
}

// NilErr represents redis nil
var NilErr = errors.New("nil")

// RedisCli is abstraction for redis client, required commands only not all commands
type RedisCli interface {
	// Eval sends lua script to redis
	// args should be string, integer or float
	// returns string, int64, []interface{} (elements can be string or int64)
	Eval(script string, keys []string, args []interface{}) (interface{}, error)
	Set(key string, value string, expiration time.Duration) error
	// Get represents redis command GET
	// please NilErr when no such key in redis
	Get(key string) (string, error)
	Del(keys []string) error
	HSet(key string, field string, value string) error
	HDel(key string, fields []string) error
	SMembers(key string) ([]string, error)
	SRem(key string, members []string) error
	ZAdd(key string, values map[string]float64) error
	ZRem(key string, fields []string) (int64, error)
	ZCard(key string) (int64, error)
	ZScore(key string, member string) (float64, error)
	LLen(key string) (int64, error)
	LRem(key string, count int64, value string) (int64, error)

	// Publish used for monitor only
	Publish(channel string, payload string) error
	// Subscribe used for monitor only
	// returns: payload channel, subscription closer, error; the subscription closer should close payload channel as well
	Subscribe(channel string) (payloads <-chan string, close func(), err error)

	// ScriptLoad call `script load` command
	ScriptLoad(script string) (string, error)
	// EvalSha run preload scripts
	// If there is no preload scripts please return error with message "NOSCRIPT"
	EvalSha(sha1 string, keys []string, args []interface{}) (interface{}, error)
}

// Logger is an abstraction of logging system
type Logger interface {
	Printf(format string, v ...interface{})
}

type hashTagKeyOpt int
type prefixOpt string

// CallbackFunc receives and consumes messages
// returns true to confirm successfully consumed, false to re-deliver this message
type CallbackFunc = func(string) bool

// UseHashTagKey add hashtags to redis keys to ensure all keys of this queue are allocated in the same hash slot.
// If you are using Codis/AliyunRedisCluster/TencentCloudRedisCluster, add this option to NewQueue
// WARNING! Changing (add or remove) this option will cause DelayQueue failing to read existed data in redis
// see more:  https://redis.io/docs/reference/cluster-spec/#hash-tags
func UseHashTagKey() interface{} {
	return hashTagKeyOpt(1)
}

// UseCustomPrefix customize prefix to instead of default prefix "dp"
func UseCustomPrefix(prefix string) interface{} {
	return prefixOpt(prefix)
}

// NewQueue0 creates a new queue, use DelayQueue.StartConsume to consume or DelayQueue.SendScheduleMsg to publish message
// callback returns true to confirm successful consumption. If callback returns false or not return within maxConsumeDuration, DelayQueue will re-deliver this message
func NewQueue0(name string, cli RedisCli, opts ...interface{}) *DelayQueue {
	if name == "" {
		panic("name is required")
	}
	if cli == nil {
		panic("cli is required")
	}
	prefix := "dp"
	useHashTag := false
	var callback CallbackFunc = nil
	for _, opt := range opts {
		switch o := opt.(type) {
		case hashTagKeyOpt:
			useHashTag = true
		case prefixOpt:
			prefix = string(o)
		case CallbackFunc:
			callback = o
		}
	}
	keyPrefix := prefix + ":" + name
	if useHashTag {
		keyPrefix = "{" + keyPrefix + "}"
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
		close:              nil,
		maxConsumeDuration: 5 * time.Second,
		msgTTL:             time.Hour,
		logger:             log.Default(),
		defaultRetryCount:  3,
		fetchInterval:      time.Second,
		concurrent:         1,
		sha1map:            make(map[string]string),
		sha1mapMu:          &sync.RWMutex{},
		scriptPreload:      true,
	}
}

// WithCallback set callback for queue to receives and consumes messages
// callback returns true to confirm successfully consumed, false to re-deliver this message
func (q *DelayQueue) WithCallback(callback CallbackFunc) *DelayQueue {
	q.cb = callback
	return q
}

// WithLogger customizes logger for queue
func (q *DelayQueue) WithLogger(logger Logger) *DelayQueue {
	q.logger = logger
	return q
}

// WithFetchInterval customizes the interval at which consumer fetch message from redis
func (q *DelayQueue) WithFetchInterval(d time.Duration) *DelayQueue {
	q.fetchInterval = d
	return q
}

// WithScriptPreload use script load command preload scripts to redis
func (q *DelayQueue) WithScriptPreload(flag bool) *DelayQueue {
	q.scriptPreload = flag
	return q
}

// WithMaxConsumeDuration customizes max consume duration
// If no acknowledge received within WithMaxConsumeDuration after message delivery, DelayQueue will try to deliver this message again
func (q *DelayQueue) WithMaxConsumeDuration(d time.Duration) *DelayQueue {
	q.maxConsumeDuration = d
	return q
}

// WithFetchLimit limits the max number of processing messages, 0 means no limit
func (q *DelayQueue) WithFetchLimit(limit uint) *DelayQueue {
	q.fetchLimit = limit
	return q
}

// WithConcurrent sets the number of concurrent consumers
func (q *DelayQueue) WithConcurrent(c uint) *DelayQueue {
	if c == 0 {
		panic("concurrent cannot be 0")
	}
	q.assertNotRunning()
	q.concurrent = c
	return q
}

// WithDefaultRetryCount customizes the max number of retry, it effects of messages in this queue
// use WithRetryCount during DelayQueue.SendScheduleMsg or DelayQueue.SendDelayMsg to specific retry count of particular message
func (q *DelayQueue) WithDefaultRetryCount(count uint) *DelayQueue {
	q.defaultRetryCount = count
	return q
}

// WithNackRedeliveryDelay customizes the interval between redelivery and nack (callback returns false)
// If consumption exceeded deadline, the message will be redelivered immediately
func (q *DelayQueue) WithNackRedeliveryDelay(d time.Duration) *DelayQueue {
	q.nackRedeliveryDelay = d
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

// MessageInfo stores information to trace a message
type MessageInfo struct {
	id string
}

func (msg *MessageInfo) ID() string {
	return msg.id
}

const (
	StatePending    = "pending"
	StateReady      = "ready"
	StateReadyRetry = "ready_to_retry"
	StateConsuming  = "consuming"
	StateUnknown    = "unknown"
)

// SendScheduleMsgV2 submits a message delivered at given time
func (q *DelayQueue) SendScheduleMsgV2(payload string, t time.Time, opts ...interface{}) (*MessageInfo, error) {
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
		return nil, fmt.Errorf("store msg failed: %v", err)
	}
	// store retry count
	err = q.redisCli.HSet(q.retryCountKey, idStr, strconv.Itoa(int(retryCount)))
	if err != nil {
		return nil, fmt.Errorf("store retry count failed: %v", err)
	}
	// put to pending
	err = q.redisCli.ZAdd(q.pendingKey, map[string]float64{idStr: float64(t.Unix())})
	if err != nil {
		return nil, fmt.Errorf("push to pending failed: %v", err)
	}
	q.reportEvent(NewMessageEvent, 1)
	return &MessageInfo{
		id: idStr,
	}, nil
}

// SendDelayMsg submits a message delivered after given duration
func (q *DelayQueue) SendDelayMsgV2(payload string, duration time.Duration, opts ...interface{}) (*MessageInfo, error) {
	t := time.Now().Add(duration)
	return q.SendScheduleMsgV2(payload, t, opts...)
}

// SendScheduleMsg submits a message delivered at given time
// It is compatible with SendScheduleMsgV2, but does not return MessageInfo
func (q *DelayQueue) SendScheduleMsg(payload string, t time.Time, opts ...interface{}) error {
	_, err := q.SendScheduleMsgV2(payload, t, opts...)
	return err
}

// SendDelayMsg submits a message delivered after given duration
// It is compatible with SendDelayMsgV2, but does not return MessageInfo
func (q *DelayQueue) SendDelayMsg(payload string, duration time.Duration, opts ...interface{}) error {
	t := time.Now().Add(duration)
	return q.SendScheduleMsg(payload, t, opts...)
}

type InterceptResult struct {
	Intercepted bool
	State       string
}

// TryIntercept trys to intercept a message
func (q *DelayQueue) TryIntercept(msg *MessageInfo) (*InterceptResult, error) {
	id := msg.ID()
	// try to intercept at ready
	removed, err := q.redisCli.LRem(q.readyKey, 0, id)
	if err != nil {
		q.logger.Printf("intercept %s from ready failed: %v", id, err)
	}
	if removed > 0 {
		_ = q.redisCli.Del([]string{q.genMsgKey(id)})
		_ = q.redisCli.HDel(q.retryCountKey, []string{id})
		return &InterceptResult{
			Intercepted: true,
			State:       StateReady,
		}, nil
	}
	// try to intercept at pending
	removed, err = q.redisCli.ZRem(q.pendingKey, []string{id})
	if err != nil {
		q.logger.Printf("intercept %s from pending failed: %v", id, err)
	}
	if removed > 0 {
		_ = q.redisCli.Del([]string{q.genMsgKey(id)})
		_ = q.redisCli.HDel(q.retryCountKey, []string{id})
		return &InterceptResult{
			Intercepted: true,
			State:       StatePending,
		}, nil
	}
	// message may be being consumed or has been successfully consumed
	// if the message has been successfully consumed, the following action will cause nothing
	// if the message is being consumed，the following action will prevent it from being retried
	q.redisCli.HDel(q.retryCountKey, []string{id})
	q.redisCli.LRem(q.retryKey, 0, id)

	return &InterceptResult{
		Intercepted: false,
		State:       StateUnknown,
	}, nil
}

func (q *DelayQueue) loadScript(script string) (string, error) {
	sha1, err := q.redisCli.ScriptLoad(script)
	if err != nil {
		return "", err
	}
	q.sha1mapMu.Lock()
	q.sha1map[script] = sha1
	q.sha1mapMu.Unlock()
	return sha1, nil
}

func (q *DelayQueue) eval(script string, keys []string, args []interface{}) (interface{}, error) {
	if !q.scriptPreload {
		return q.redisCli.Eval(script, keys, args)
	}
	var err error
	q.sha1mapMu.RLock()
	sha1, ok := q.sha1map[script]
	q.sha1mapMu.RUnlock()
	if !ok {
		sha1, err = q.loadScript(script)
		if err != nil {
			return nil, err
		}
	}
	result, err := q.redisCli.EvalSha(sha1, keys, args)
	if err == nil {
		return result, err
	}
	// script not loaded, reload it
	// It is possible to access a node in the cluster that has no pre-loaded scripts.
	if strings.HasPrefix(err.Error(), "NOSCRIPT") {
		sha1, err = q.loadScript(script)
		if err != nil {
			return nil, err
		}
		// try again
		result, err = q.redisCli.EvalSha(sha1, keys, args)
	}
	return result, err
}

// pending2ReadyScript atomically moves messages from pending to ready
// keys: pendingKey, readyKey
// argv: currentTime
// returns: ready message number
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
if (#args2 > 0) then 
	redis.call('LPush', KEYS[2], unpack(args2))
end
redis.call('ZRemRangeByScore', KEYS[1], '0', ARGV[1])  -- remove msgs from pending
return #msgs
`

func (q *DelayQueue) pending2Ready() error {
	now := time.Now().Unix()
	keys := []string{q.pendingKey, q.readyKey}
	raw, err := q.eval(pending2ReadyScript, keys, []interface{}{now})
	if err != nil && err != NilErr {
		return fmt.Errorf("pending2ReadyScript failed: %v", err)
	}
	count, ok := raw.(int64)
	if ok {
		q.reportEvent(ReadyEvent, int(count))
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
	ret, err := q.eval(ready2UnackScript, keys, []interface{}{retryTime})
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
	q.reportEvent(DeliveredEvent, 1)
	return str, nil
}

func (q *DelayQueue) retry2Unack() (string, error) {
	retryTime := time.Now().Add(q.maxConsumeDuration).Unix()
	keys := []string{q.retryKey, q.unAckKey}
	ret, err := q.eval(ready2UnackScript, keys, []interface{}{retryTime, q.retryKey, q.unAckKey})
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

func (q *DelayQueue) ack(idStr string) error {
	atomic.AddInt32(&q.fetchCount, -1)
	_, err := q.redisCli.ZRem(q.unAckKey, []string{idStr})
	if err != nil {
		return fmt.Errorf("remove from unack failed: %v", err)
	}
	// msg key has ttl, ignore result of delete
	_ = q.redisCli.Del([]string{q.genMsgKey(idStr)})
	_ = q.redisCli.HDel(q.retryCountKey, []string{idStr})
	q.reportEvent(AckEvent, 1)
	return nil
}

// updateZSetScoreScript update score of a zset member if it exists
// KEYS[1]: zset
// ARGV[1]: score
// ARGV[2]: member
const updateZSetScoreScript = `
if redis.call('zrank', KEYS[1], ARGV[2]) ~= nil then
    return redis.call('zadd', KEYS[1], ARGV[1], ARGV[2])
else
    return 0
end
`

func (q *DelayQueue) updateZSetScore(key string, score float64, member string) error {
	scoreStr := strconv.FormatFloat(score, 'f', -1, 64)
	_, err := q.eval(updateZSetScoreScript, []string{key}, []interface{}{scoreStr, member})
	return err
}

func (q *DelayQueue) nack(idStr string) error {
	atomic.AddInt32(&q.fetchCount, -1)
	retryTime := float64(time.Now().Add(q.nackRedeliveryDelay).Unix())
	// if message consumption has not reach deadlin (still in unAckKey), then update its retry time
	err := q.updateZSetScore(q.unAckKey, retryTime, idStr)
	if err != nil {
		return fmt.Errorf("negative ack failed: %v", err)
	}
	q.reportEvent(NackEvent, 1)
	return nil
}

// unack2RetryScript atomically moves messages from unack to retry which remaining retry count greater than 0,
// and moves messages from unack to garbage which  retry count is 0
// Because DelayQueue cannot determine garbage message before eval unack2RetryScript, so it cannot pass keys parameter to redisCli.Eval
// Therefore unack2RetryScript moves garbage message to garbageKey instead of deleting directly
// keys: unackKey, retryCountKey, retryKey, garbageKey
// argv: currentTime
// returns: {retryMsgs, failMsgs}
const unack2RetryScript = `
local unack2retry = function(msgs)
	local retryCounts = redis.call('HMGet', KEYS[2], unpack(msgs)) -- get retry count
	local retryMsgs = 0
	local failMsgs = 0
	for i,v in ipairs(retryCounts) do
		local k = msgs[i]
		if v ~= false and v ~= nil and v ~= '' and tonumber(v) > 0 then
			redis.call("HIncrBy", KEYS[2], k, -1) -- reduce retry count
			redis.call("LPush", KEYS[3], k) -- add to retry
			retryMsgs = retryMsgs + 1
		else
			redis.call("HDel", KEYS[2], k) -- del retry count
			redis.call("SAdd", KEYS[4], k) -- add to garbage
			failMsgs = failMsgs + 1
		end
	end
	return retryMsgs, failMsgs
end

local retryMsgs = 0
local failMsgs = 0
local msgs = redis.call('ZRangeByScore', KEYS[1], '0', ARGV[1])  -- get retry msg
if (#msgs == 0) then return end
if #msgs < 4000 then
	local d1, d2 = unack2retry(msgs)
	retryMsgs = retryMsgs + d1
	failMsgs = failMsgs + d2
else
	local buf = {}
	for _,v in ipairs(msgs) do
		table.insert(buf, v)
		if #buf == 4000 then
		    local d1, d2 = unack2retry(buf)
			retryMsgs = retryMsgs + d1
			failMsgs = failMsgs + d2
			buf = {}
		end
	end
	if (#buf > 0) then
		local d1, d2 = unack2retry(buf)
		retryMsgs = retryMsgs + d1
		failMsgs = failMsgs + d2
	end
end
redis.call('ZRemRangeByScore', KEYS[1], '0', ARGV[1])  -- remove msgs from unack
return {retryMsgs, failMsgs}
`

func (q *DelayQueue) unack2Retry() error {
	keys := []string{q.unAckKey, q.retryCountKey, q.retryKey, q.garbageKey}
	now := time.Now()
	raw, err := q.eval(unack2RetryScript, keys, []interface{}{now.Unix()})
	if err != nil && err != NilErr {
		return fmt.Errorf("unack to retry script failed: %v", err)
	}
	infos, ok := raw.([]interface{})
	if ok && len(infos) == 2 {
		retryCount, ok := infos[0].(int64)
		if ok {
			q.reportEvent(RetryEvent, int(retryCount))
		}
		failCount, ok := infos[1].(int64)
		if ok {
			q.reportEvent(FinalFailedEvent, int(failCount))
		}
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

func (q *DelayQueue) beforeConsume() ([]string, error) {
	// pending to ready
	err := q.pending2Ready()
	if err != nil {
		return nil, err
	}
	// ready2Unack
	// prioritize new message consumption to avoid avalanches
	ids := make([]string, 0, q.fetchLimit)
	var fetchCount int32
	for {
		fetchCount = atomic.LoadInt32(&q.fetchCount)
		if q.fetchLimit > 0 && fetchCount >= int32(q.fetchLimit) {
			break
		}
		idStr, err := q.ready2Unack()
		if err == NilErr { // consumed all
			break
		}
		if err != nil {
			return nil, err
		}
		ids = append(ids, idStr)
		atomic.AddInt32(&q.fetchCount, 1)
	}
	// retry2Unack
	if fetchCount < int32(q.fetchLimit) || q.fetchLimit == 0 {
		for {
			fetchCount = atomic.LoadInt32(&q.fetchCount)
			if q.fetchLimit > 0 && fetchCount >= int32(q.fetchLimit) {
				break
			}
			idStr, err := q.retry2Unack()
			if err == NilErr { // consumed all
				break
			}
			if err != nil {
				return nil, err
			}
			ids = append(ids, idStr)
			atomic.AddInt32(&q.fetchCount, 1)
		}
	}
	return ids, nil
}

func (q *DelayQueue) afterConsume() error {
	// unack to retry
	err := q.unack2Retry()
	if err != nil {
		return err
	}
	err = q.garbageCollect()
	if err != nil {
		return err
	}
	return nil
}

func (q *DelayQueue) setRunning() {
	atomic.StoreInt32(&q.running, 1)
}

func (q *DelayQueue) setNotRunning() {
	atomic.StoreInt32(&q.running, 0)
}

func (q *DelayQueue) assertNotRunning() {
	running := atomic.LoadInt32(&q.running)
	if running > 0 {
		panic("operation cannot be performed during running")
	}
}

func (q *DelayQueue) goWithRecover(fn func()) {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				q.logger.Printf("panic: %v\n", err)
			}
		}()
		fn()
	}()
}

// StartConsume creates a goroutine to consume message from DelayQueue
// use `<-done` to wait consumer stopping
// If there is no callback set, StartConsume will panic
func (q *DelayQueue) StartConsume() (done <-chan struct{}) {
	if q.cb == nil {
		panic("this instance has no callback")
	}
	q.close = make(chan struct{}, 1)
	q.setRunning()
	q.ticker = time.NewTicker(q.fetchInterval)
	q.consumeBuffer = make(chan string, q.fetchLimit)
	done0 := make(chan struct{})
	// start worker
	for i := 0; i < int(q.concurrent); i++ {
		q.goWithRecover(func() {
			for id := range q.consumeBuffer {
				q.callback(id)
				q.afterConsume()
			}
		})
	}
	// start main loop
	go func() {
	tickerLoop:
		for {
			select {
			case <-q.ticker.C:
				ids, err := q.beforeConsume()
				if err != nil {
					q.logger.Printf("before consume error: %v", err)
				}
				q.goWithRecover(func() {
					for _, id := range ids {
						q.consumeBuffer <- id
					}
				})
				// Always do unack2Retry and garbageCollect even there is no new messages
				// https://github.com/HDT3213/delayqueue/issues/21
				err = q.afterConsume()
				if err != nil {
					q.logger.Printf("after consume error: %v", err)
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
	q.setNotRunning()
	if q.ticker != nil {
		q.ticker.Stop()
	}
}

// GetPendingCount returns the number of pending messages
func (q *DelayQueue) GetPendingCount() (int64, error) {
	return q.redisCli.ZCard(q.pendingKey)
}

// GetReadyCount returns the number of messages which have arrived delivery time but but have not been delivered
func (q *DelayQueue) GetReadyCount() (int64, error) {
	return q.redisCli.LLen(q.readyKey)
}

// GetProcessingCount returns the number of messages which are being processed
func (q *DelayQueue) GetProcessingCount() (int64, error) {
	return q.redisCli.ZCard(q.unAckKey)
}

// EventListener which will be called when events occur
// This Listener can be used to monitor running status
type EventListener interface {
	// OnEvent will be called when events occur
	OnEvent(*Event)
}

// ListenEvent register a listener which will be called when events occur,
// so it can be used to monitor running status
//
// But It can ONLY receive events from the CURRENT INSTANCE,
// if you want to listen to all events in queue, just use Monitor.ListenEvent
//
// There can be AT MOST ONE EventListener in an DelayQueue instance.
// If you are using customized listener, Monitor will stop working
func (q *DelayQueue) ListenEvent(listener EventListener) {
	q.eventListener = listener
}

// RemoveListener stops reporting events to EventListener
func (q *DelayQueue) DisableListener() {
	q.eventListener = nil
}

func (q *DelayQueue) reportEvent(code int, count int) {
	listener := q.eventListener // eventListener may be changed during running
	if listener != nil && count > 0 {
		event := &Event{
			Code:      code,
			Timestamp: time.Now().Unix(),
			MsgCount:  count,
		}
		listener.OnEvent(event)
	}
}

// pubsubListener receives events and reports them through redis pubsub for monitoring
type pubsubListener struct {
	redisCli   RedisCli
	reportChan string
}

func genReportChannel(name string) string {
	return "dq:" + name + ":reportEvents"
}

// EnableReport enables reporting to monitor
func (q *DelayQueue) EnableReport() {
	reportChan := genReportChannel(q.name)
	q.ListenEvent(&pubsubListener{
		redisCli:   q.redisCli,
		reportChan: reportChan,
	})
}

// DisableReport stops reporting to monitor
func (q *DelayQueue) DisableReport() {
	q.DisableListener()
}

func (l *pubsubListener) OnEvent(event *Event) {
	payload := encodeEvent(event)
	l.redisCli.Publish(l.reportChan, payload)
}
