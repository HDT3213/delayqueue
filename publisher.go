package delayqueue

import (
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

// Publisher only publishes messages to delayqueue, it is a encapsulation of delayqueue
type Publisher struct {
	inner *DelayQueue
}

// NewPublisher0 creates a new Publisher by a RedisCli instance
func NewPublisher0(name string, cli RedisCli, opts ...interface{}) *Publisher {
	return &Publisher{
		inner: NewQueue0(name, cli, opts...),
	}
}

// NewPublisher creates a new Publisher by a *redis.Client
func NewPublisher(name string, cli *redis.Client, opts ...interface{}) *Publisher {
	rc := &redisV9Wrapper{
		inner: cli,
	}
	return NewPublisher0(name, rc, opts...)
}

// WithLogger customizes logger for queue
func (p *Publisher) WithLogger(logger *log.Logger) *Publisher {
	p.inner.logger = logger
	return p
}

// SendScheduleMsg submits a message delivered at given time
func (p *Publisher) SendScheduleMsg(payload string, t time.Time, opts ...interface{}) error {
	return p.inner.SendScheduleMsg(payload, t, opts...)
}

// SendDelayMsg submits a message delivered after given duration
func (p *Publisher) SendDelayMsg(payload string, duration time.Duration, opts ...interface{}) error {
	return p.inner.SendDelayMsg(payload, duration, opts...)
}
