# DelayQueue

![license](https://img.shields.io/github/license/HDT3213/delayqueue)
[![Build Status](https://travis-ci.com/HDT3213/delayqueue.svg?branch=master)](https://app.travis-ci.com/github/HDT3213/delayqueue)
[![Coverage Status](https://coveralls.io/repos/github/HDT3213/delayqueue/badge.svg?branch=master)](https://coveralls.io/github/HDT3213/delayqueue?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/HDT3213/delayqueue)](https://goreportcard.com/report/github.com/HDT3213/delayqueue)
[![Go Reference](https://pkg.go.dev/badge/github.com/hdt3213/delayqueue.svg)](https://pkg.go.dev/github.com/hdt3213/delayqueue)

[中文版](https://github.com/HDT3213/delayqueue/blob/master/README_CN.md)

DelayQueue is a message queue supporting delayed/scheduled delivery based on redis. It is designed to be reliable, scalable and easy to get started.

Core Advantages:

- Guaranteed at least once consumption
- Auto retry failed messages
- Works out of the box, Config Nothing and Deploy Nothing, A Redis is all you need.
- Natively adapted to the distributed environment, messages processed concurrently on multiple machines
. Workers can be added, removed or migrated at any time
- Support Redis Cluster for high availability

## Install

DelayQueue requires a Go version with modules support. Run following command line in your project with go.mod:

```
go get github.com/hdt3213/delayqueue
```

## Get Started

```go
package main

import (
	"github.com/redis/go-redis/v9"
	"github.com/hdt3213/delayqueue"
	"strconv"
	"time"
)

func main() {
	redisCli := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	queue := delayqueue.NewQueue("example", redisCli, func(payload string) bool {
		// callback returns true to confirm successful consumption.
		// If callback returns false or not return within maxConsumeDuration, DelayQueue will re-deliver this message
		return true
	}).WithConcurrent(4) // set the number of concurrent consumers 
	// send delay message
	for i := 0; i < 10; i++ {
		err := queue.SendDelayMsg(strconv.Itoa(i), time.Hour, delayqueue.WithRetryCount(3))
		if err != nil {
			panic(err)
		}
	}
	// send schedule message
	for i := 0; i < 10; i++ {
		err := queue.SendScheduleMsg(strconv.Itoa(i), time.Now().Add(time.Hour))
		if err != nil {
			panic(err)
		}
	}
	// start consume
	done := queue.StartConsume()
	<-done
}
```

> if you are using github.com/go-redis/redis/v8 please use `go get github.com/hdt3213/delayqueue@v8`
> If you are using redis client other than go-redis, you could wrap your redis client into [RedisCli](https://pkg.go.dev/github.com/hdt3213/delayqueue#RedisCli) interface

## Producer consumer distributed deployment

By default, delayqueue instances can be both producers and consumers. If your program only need producers and consumers are placed elsewhere, `delayqueue.NewProducer` is a good option for you.

```go
func consumer() {
	queue := NewQueue("test", redisCli, cb)
	queue.StartConsume()
}

func producer() {
	publisher := NewPublisher("test", redisCli)
	publisher.SendDelayMsg(strconv.Itoa(i), 0)
}
```

## Options

```go
WithLogger(logger *log.Logger)
```

WithLogger customizes logger for queue


```go
WithConcurrent(c uint) 
```

WithConcurrent sets the number of concurrent consumers

```go
WithFetchInterval(d time.Duration)
```

WithFetchInterval customizes the interval at which consumer fetch message from redis

```go
WithMaxConsumeDuration(d time.Duration)
```

WithMaxConsumeDuration customizes max consume duration

If no acknowledge received within WithMaxConsumeDuration after message delivery, DelayQueue will try to deliver this
message again

```go
WithFetchLimit(limit uint)
```

WithFetchLimit limits the max number of unack (processing) messages

```go
UseHashTagKey()
```

UseHashTagKey add hashtags to redis keys to ensure all keys of this queue are allocated in the same hash slot.

If you are using Codis/AliyunRedisCluster/TencentCloudRedisCluster, you should add this option to NewQueue: `NewQueue("test", redisCli, cb, UseHashTagKey())`. This Option cannot be changed after DelayQueue has been created.

WARNING! CHANGING(add or remove) this option will cause DelayQueue failing to read existed data in redis

> see more:  https://redis.io/docs/reference/cluster-spec/#hash-tags

```go
WithDefaultRetryCount(count uint)
```

WithDefaultRetryCount customizes the max number of retry, it effects of messages in this queue

use WithRetryCount during DelayQueue.SendScheduleMsg or DelayQueue.SendDelayMsg to specific retry count of particular message

## Cluster

If you are using Redis Cluster, please use `NewQueueOnCluster`

```go
redisCli := redis.NewClusterClient(&redis.ClusterOptions{
    Addrs: []string{
        "127.0.0.1:7000",
        "127.0.0.1:7001",
        "127.0.0.1:7002",
    },
})
callback := func(s string) bool {
    return true
}
queue := NewQueueOnCluster("test", redisCli, callback)
```

If you are using transparent clusters, such as codis, twemproxy, or the redis of cluster architecture on aliyun, tencentcloud,
just use `NewQueue` and enable hash tag

```go
redisCli := redis.NewClient(&redis.Options{
    Addr: "127.0.0.1:6379",
})
callback := func(s string) bool {
    return true
}
queue := delayqueue.NewQueue("example", redisCli, callback, UseHashTagKey())
```

## More Details

Here is the complete flowchart:

![](https://s2.loli.net/2022/09/10/tziHmcAX4sFJPN6.png)

- pending: A sorted set of messages pending for delivery. `member` is message id, `score` is delivery unix timestamp.
- ready: A list of messages ready to deliver. Workers fetch messages from here.
- unack: A sorted set of messages waiting for ack (successfully consumed confirmation) which means the messages here is being processing. `member` is message id, `score` is the unix timestamp of processing deadline.
- retry: A list of messages which processing exceeded deadline and waits for retry
- garbage: A list of messages reaching max retry count and waits for cleaning 