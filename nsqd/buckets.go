package nsqd

import (
	"fmt"
	"sync"
	"time"
)

var round time.Duration = 2 * time.Second

type Host struct {
	host                                    string
	lastHeardFromAt                         time.Time
	buckets                                 map[time.Time]*Bucket
	nextBucket                              *Bucket
	recoveryLock, bucketsLock, messagesLock *sync.Mutex
	messages                                map[MessageID]*Bucket
	inRecovery                              bool
	InitiateRecovery                        func()
}

func NewHost(hostname string) *Host {
	h := &Host{
		host:         hostname,
		buckets:      map[time.Time]*Bucket{},
		bucketsLock:  &sync.Mutex{},
		messagesLock: &sync.Mutex{},
		recoveryLock: &sync.Mutex{},
		messages:     map[MessageID]*Bucket{},
	}
	h.nextBucket = h.GetBucketAtExpireTime(time.Now())
	return h
}

type Bucket struct {
	messages   map[MessageID]Message
	expiration time.Time
	next       *Bucket
	host       *Host
}

func (b *Bucket) GetMessage(id MessageID) Message {
	return b.messages[id]
}

func (h *Host) GetBucketAtExpireTime(e time.Time) *Bucket {
	rounded := e.Round(round)

	h.bucketsLock.Lock()
	b, ok := h.buckets[rounded]
	h.bucketsLock.Unlock()

	if !ok {
		b = &Bucket{
			messages:   make(map[MessageID]Message),
			expiration: e,
		}

		// is this bucket the earliest?
		if e.Before(h.nextBucket.expiration) {
			b.next = h.nextBucket
			h.nextBucket = b
		}

		// what is the previous?
		prev := h.GetBucketAtExpireTime(rounded.Add(-round))
		prev.next = b

		h.bucketsLock.Lock()
		h.buckets[rounded] = b
		h.bucketsLock.Unlock()
	}
	return b
}

func (h *Host) AddMessage(m Message, e time.Time) {
	if h == nil {
		return
	}
	h.RemoveMessage(m)
	h.GetBucketAtExpireTime(e).messages[m.ID] = m
}

func (h *Host) RemoveMessage(m Message) {
	if h == nil {
		return
	}
	h.messagesLock.Lock()
	defer h.messagesLock.Unlock()
	if have, ok := h.messages[m.ID]; ok {
		delete(have.messages, m.ID)
		delete(h.messages, m.ID)
	}
}

func (h *Host) GetMessages() map[MessageID]*Bucket {
	m := make(map[MessageID]*Bucket, len(h.messages))
	h.messagesLock.Lock()
	defer h.messagesLock.Unlock()
	for mid, b := range h.messages {
		m[mid] = b
	}
	return m
}

func (h *Host) Recovery(stop chan bool) {
	if h == nil {
		return
	}
	ticker := time.NewTicker(round)
	for {
		select {
		case <-ticker.C:
			expire := h.nextBucket
			h.nextBucket = expire.next
			expire.Expire()
		case <-stop:
			return
		}
	}
}

// SetRecovery sets the recovery state of the host atomically. If the state was changed, the return value is true.
func (h *Host) SetRecovery(state bool) bool {
	h.recoveryLock.Lock()
	defer h.recoveryLock.Unlock()
	if h.inRecovery == state {
		return false
	}
	h.inRecovery = state
	return true
}

func (h *Host) RecoveryTopic() string {
	return fmt.Sprintf("recover:%s", h.host)
}

func (b *Bucket) Expire() {
	if len(b.messages) > 0 {
		go b.host.InitiateRecovery()
	}
}
