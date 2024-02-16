package delayqueue

import (
	"errors"
	"strconv"
	"strings"
)

const (
	// NewMessageEvent emmited when send message
	NewMessageEvent = iota + 1
	// ReadyEvent emmited when messages has reached delivery time
	ReadyEvent
	// DeliveredEvent emmited when messages has been delivered to consumer
	DeliveredEvent
	// AckEvent emmited when receive message successfully consumed callback
	AckEvent
	// AckEvent emmited when receive message consumption failure callback
	NackEvent
	// RetryEvent emmited when message re-delivered to consumer
	RetryEvent
	// FinalFailedEvent emmited when message reaches max retry attempts
	FinalFailedEvent
)

// Event contains internal event information during the queue operation and can be used to monitor the queue status.
type Event struct {
	// Code represents event type, such as NewMessageEvent, ReadyEvent
	Code int
	// Timestamp is the event time
	Timestamp int64
	// MsgCount represents the number of messages related to the event
	MsgCount int
}

func encodeEvent(e *Event) string {
	return strconv.Itoa(e.Code) +
		" " + strconv.FormatInt(e.Timestamp, 10) +
		" " + strconv.Itoa(e.MsgCount)
}

func decodeEvent(payload string) (*Event, error) {
	items := strings.Split(payload, " ")
	if len(items) != 3 {
		return nil, errors.New("[decode event error! wrong item count, payload: " + payload)
	}
	code, err := strconv.Atoi(items[0])
	if err != nil {
		return nil, errors.New("decode event error! wrong event code, payload: " + payload)
	}
	timestamp, err := strconv.ParseInt(items[1], 10, 64)
	if err != nil {
		return nil, errors.New("decode event error! wrong timestamp, payload: " + payload)
	}
	count, err := strconv.Atoi(items[2])
	if err != nil {
		return nil, errors.New("decode event error! wrong msg count, payload: " + payload)
	}
	return &Event{
		Code:      code,
		Timestamp: timestamp,
		MsgCount:  count,
	}, nil
}
