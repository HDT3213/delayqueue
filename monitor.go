package delayqueue

import (
	"log"

	"github.com/redis/go-redis/v9"
)

// Monitor can get running status and events of DelayQueue
type Monitor struct {
	inner *DelayQueue
}

// NewMonitor0 creates a new Monitor by a RedisCli instance
func NewMonitor0(name string, cli RedisCli, opts ...interface{}) *Monitor {
	return &Monitor{
		inner: NewQueue0(name, cli, opts...),
	}
}

// NewPublisher creates a new Publisher by a *redis.Client
func NewMonitor(name string, cli *redis.Client, opts ...interface{}) *Monitor {
	rc := &redisV9Wrapper{
		inner: cli,
	}
	return NewMonitor0(name, rc, opts...)
}

// WithLogger customizes logger for queue
func (m *Monitor) WithLogger(logger *log.Logger) *Monitor {
	m.inner.logger = logger
	return m
}

// GetPendingCount returns the number of messages which delivery time has not arrived
func (m *Monitor) GetPendingCount() (int64, error) {
	return m.inner.GetPendingCount()
}

// GetReadyCount returns the number of messages which have arrived delivery time but but have not been delivered yet
func (m *Monitor) GetReadyCount() (int64, error) {
	return m.inner.GetReadyCount()
}

// GetProcessingCount returns the number of messages which are being processed
func (m *Monitor) GetProcessingCount() (int64, error) {
	return m.inner.GetProcessingCount()
}

// ListenEvent register a listener which will be called when events occured in this queue
// so it can be used to monitor running status
// returns: close function, error
func (m *Monitor) ListenEvent(listener EventListener) (func(), error) {
	reportChan := genReportChannel(m.inner.name)
	sub, closer, err := m.inner.redisCli.Subscribe(reportChan)
	if err != nil {
		return nil, err
	}
	go func() {
		for payload := range sub {
			event, err := decodeEvent(payload)
			if err != nil {
				m.inner.logger.Printf("[listen event] %v\n", event)
			} else {
				listener.OnEvent(event)
			}
		}
	}()
	return closer, nil
}

