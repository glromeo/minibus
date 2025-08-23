package minibus

import (
	"sync"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type Hub struct {
	queuesMu sync.RWMutex
	queues   map[string]*queue
	topicsMu sync.RWMutex
	topics   map[string]*topic
	bufSz    int
}

func newHub(config Config) *Hub {
	return &Hub{
		queues: make(map[string]*queue),
		topics: make(map[string]*topic),
		bufSz:  config.BufferSize,
	}
}

func (h *Hub) getQueue(name string) *queue {
	h.queuesMu.RLock()
	q := h.queues[name]
	h.queuesMu.RUnlock()
	if q != nil {
		return q
	}
	h.queuesMu.Lock()
	defer h.queuesMu.Unlock()
	if q = h.queues[name]; q == nil {
		q = newQueue(name, h.bufSz)
		h.queues[name] = q
	}
	return q
}

func (h *Hub) getTopic(name string) *topic {
	h.topicsMu.RLock()
	t := h.topics[name]
	h.topicsMu.RUnlock()
	if t != nil {
		return t
	}
	h.topicsMu.Lock()
	defer h.topicsMu.Unlock()
	if t = h.topics[name]; t == nil {
		t = newTopic(name, h.bufSz)
		h.topics[name] = t
	}
	return t
}
