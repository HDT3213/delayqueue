
![license](https://img.shields.io/github/license/HDT3213/delayqueue)
[![Build Status](https://travis-ci.com/HDT3213/delayqueue.svg?branch=master)](https://app.travis-ci.com/github/HDT3213/delayqueue)
[![Coverage Status](https://coveralls.io/repos/github/HDT3213/delayqueue/badge.svg?branch=master)](https://coveralls.io/github/HDT3213/delayqueue?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/HDT3213/delayqueue)](https://goreportcard.com/report/github.com/HDT3213/delayqueue)
[![Go Reference](https://pkg.go.dev/badge/github.com/hdt3213/delayqueue.svg)](https://pkg.go.dev/github.com/hdt3213/delayqueue)

DelayQueue 是使用 Go 语言基于 Redis 实现的支持延时/定时投递的消息队列。

DelayQueue 的主要优势：
- 保证至少消费一次(At-Least-Once)
- 自动重试处理失败的消息
- 开箱即用, 无需部署或安装中间件, 只需要一个 Redis 即可工作
- 原生适配分布式环境, 可在多台机器上并发的处理消息. 可以随时增加、减少或迁移 Worker
- 支持各类 Redis 集群

## 安装

在启用了 go mod 的项目中运行下列命令即可完成安装：

```shell
go get github.com/hdt3213/delayqueue
```

> 如果您仍在使用 `github.com/go-redis/redis/v8` 请安装 `go get github.com/hdt3213/delayqueue@v8`

## 开始使用

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
		// return true 表示成功消费
		// 如果返回了 false 或者在 maxConsumeDuration 时限内没有返回则视为消费失败，DelayQueue 会重新投递消息
		return true
	}).WithConcurrent(4) // 设置消费者并发数
	// 发送延时投递消息
	for i := 0; i < 10; i++ {
		err := queue.SendDelayMsg(strconv.Itoa(i), time.Hour, delayqueue.WithRetryCount(3))
		if err != nil {
			panic(err)
		}
	}
	// 发送定时投递消息
	for i := 0; i < 10; i++ {
		err := queue.SendScheduleMsg(strconv.Itoa(i), time.Now().Add(time.Hour))
		if err != nil {
			panic(err)
		}
	}
	// 开始消费
	done := queue.StartConsume()
	<-done // 如需等待消费者关闭，监听 done 即可 
}
```

> 如果您仍在使用 redis/v8 请使用 v8 分支: `go get github.com/hdt3213/delayqueue@v8`
> 如果您在使用其他的 redis 客户端, 可以将其包装到 [RedisCli](https://pkg.go.dev/github.com/hdt3213/delayqueue#RedisCli) 接口中

## 分开部署生产者和消费者

默认情况下 delayqueue 实例既可以做生产者也可以做消费者。如果某些程序只需要发送消息，消费者部署在其它程序中，那么可以使用 `delayqueue.NewProducer`.

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

## 选项

```go
WithLogger(logger *log.Logger)
```

为 DelayQueue 设置 logger


```go
WithConcurrent(c uint) 
```

设置消费者并发数

```go
WithFetchInterval(d time.Duration)
```

设置消费者从 Redis 拉取消息的时间间隔

```go
WithMaxConsumeDuration(d time.Duration)
```

设置最长消费时间。若拉取消息后超出 MaxConsumeDuration 时限仍未返回 ACK 则认为消费失败，DelayQueue 会重新投递此消息。

```go
WithFetchLimit(limit uint)
```

FetchLimit 限制消费者从 Redis 中拉取的消息数目，即单个消费者正在处理中的消息数不会超过 FetchLimit

```go
UseHashTagKey()
```

UseHashTagKey() 会在 Redis Key 上添加 hash tag 确保同一个队列的所有 Key 分布在同一个哈希槽中。

如果您正在使用 Codis/阿里云/腾讯云等 Redis 集群，请在 NewQueue 时添加这个选项：`NewQueue("test", redisCli, cb, UseHashTagKey())`。UseHashTagKey 选项在队列创建后禁止修改。

**注意:** 修改(添加或移除)此选项会导致无法访问 Redis 中已有的数据。

see more: https://redis.io/docs/reference/cluster-spec/#hash-tags

```go
WithDefaultRetryCount(count uint)
```

设置队列中消息的默认重试次数。

在调用  DelayQueue.SendScheduleMsg or DelayQueue.SendDelayMsg 发送消息时，可以调用 WithRetryCount 为这条消息单独指定重试次数。

## 集群

如果需要在 Redis Cluster 上工作, 请使用 `NewQueueOnCluster`:

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

如果是阿里云,腾讯云的 Redis 集群版或 codis, twemproxy 这类透明式的集群, 使用 `NewQueue` 并启用 UseHashTagKey() 即可:

```go
redisCli := redis.NewClient(&redis.Options{
    Addr: "127.0.0.1:6379",
})
callback := func(s string) bool {
    return true
}
queue := delayqueue.NewQueue("example", redisCli, callback, UseHashTagKey())
```

## 更多细节

完整流程如图所示：

![](https://s2.loli.net/2022/09/10/tziHmcAX4sFJPN6.png)


整个消息队列中一共有 7 个 Redis 数据结构:

- pending: 有序集合类型，存储未到投递时间的消息。 member 为消息 ID、score 为投递时间。
- ready: 列表类型，存储已到投递时间的消息。element 为消息 ID。
- unack: 有序集合类型, 存储已投递但未确认成功消费的消息 ID。 member 为消息 ID、score 为处理超时时间, 超出这个时间还未 ack 的消息会被重试。
- retry: 列表类型，存储处理超时后等待重试的消息 ID。element 为消息 ID。
- garbage: 集合类型，用于暂存已达重试上限的消息 ID。后面介绍 unack2retry 时会介绍为什么需要 garbage 结构。
- msgKey: 为了避免两条内容完全相同的消息造成意外的影响，我们将每条消息放到一个字符串类型的键中，并分配一个 UUID 作为它的唯一标识。其它数据结构中只存储 UUID 而不存储完整的消息内容。每个 msg 拥有一个独立的 key 而不是将所有消息放到一个哈希表中是为了利用 TTL 机制避免泄漏。
- retryCountKey: 哈希表类型，键为消息 ID, 值为剩余的重试次数。

如上图所示整个消息队列中一共涉及 6 个操作：

- send: 发送一条新消息。首先存储消息内容和重试次数，并将消息 ID 放入 pending 中。
- pending2ready: 将已到投递时间的消息从 pending 移动到 ready 中
- ready2unack: 将一条等待投递的消息从 ready （或 retry） 移动到 unack 中，并把消息发送给消费者。
- unack2retry: 将 unack 中未到重试次数上限的消息转移到 retry 中，已到重试次数上限的转移到 garbage 中等待后续清理。
- ack: 从 unack 中删除处理成功的消息并清理它的 msgKey 和 retryCount 数据。
- garbageCollect: 清理已到最大重试次数的消息。



