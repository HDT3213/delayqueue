package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/hdt3213/delayqueue"
	"github.com/redis/go-redis/v9"
)

type Metrics struct {
	ProduceCount int
	DeliverCount int
	ConsumeCount int
	RetryCount   int
	FailCount    int
}

type MyProfiler struct {
	List  []*Metrics
	Start int64
}

func (p *MyProfiler) OnEvent(event *delayqueue.Event) {
	sinceUptime := event.Timestamp - p.Start
	upMinutes := sinceUptime / 60
	if len(p.List) <= int(upMinutes) {
		p.List = append(p.List, &Metrics{})
	}
	current := p.List[upMinutes]
	switch event.Code {
	case delayqueue.NewMessageEvent:
		current.ProduceCount += event.MsgCount
	case delayqueue.DeliveredEvent:
		current.DeliverCount += event.MsgCount
	case delayqueue.AckEvent:
		current.ConsumeCount += event.MsgCount
	case delayqueue.RetryEvent:
		current.RetryCount += event.MsgCount
	case delayqueue.FinalFailedEvent:
		current.FailCount += event.MsgCount
	}
}

func main() {
	redisCli := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	queue := delayqueue.NewQueue("example", redisCli, func(payload string) bool {
		return true
	})
	start := time.Now()
	queue.EnableReport()

	// setup monitor
	monitor := delayqueue.NewMonitor("example", redisCli)
	listener := &MyProfiler{
		Start: start.Unix(),
	}
	monitor.ListenEvent(listener)

	// print metrics every minute
	tick := time.Tick(time.Minute)
	go func() {
		for range tick {
			minutes := len(listener.List)-1
			fmt.Printf("%d: %#v", minutes, listener.List[minutes])
		}
	}()

	// start test
	for i := 0; i < 10; i++ {
		err := queue.SendDelayMsg(strconv.Itoa(i), 0, delayqueue.WithRetryCount(3))
		if err != nil {
			panic(err)
		}
	}
	done := queue.StartConsume()
	<-done
}
