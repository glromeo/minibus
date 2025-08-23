package minibus

import (
	"context"
	"sync"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

type handler interface{ remove() }

type client struct {
	ws     *websocket.Conn
	wsMu   sync.Mutex // serialize writes
	hub    *Hub
	subsMu sync.Mutex
	subs   map[string]handler // key = kind:name
}

func (c *client) writeJSON(ctx context.Context, f any) error {
	c.wsMu.Lock()
	defer c.wsMu.Unlock()
	return wsjson.Write(ctx, c.ws, f)
}

func (c *client) subscribe(kind Kind, name string, once bool) {
	var h handler
	if kind == "queue" {
		q := c.hub.getQueue(name)
		h = q.addConsumer(c, once)
	} else {
		t := c.hub.getTopic(name)
		h = t.addSubscriber(c, once)
	}

	// avoids collisions: "queue:jobs" vs "topic:jobs"
	qn := string(kind) + ":" + name

	// If already subscribed, remove old handle first (avoid duplicates/leaks)
	var old handler
	c.subsMu.Lock()
	if prev, ok := c.subs[qn]; ok {
		old = prev
	}
	c.subs[qn] = h
	c.subsMu.Unlock()

	if old != nil {
		old.remove()
	}
}

func (c *client) unsubscribe(kind Kind, name string) {
	qn := string(kind) + ":" + name

	// Take the handle without holding the lock during remove()
	c.subsMu.Lock()
	h := c.subs[qn]
	delete(c.subs, qn)
	c.subsMu.Unlock()

	if h != nil {
		h.remove()
	}
}

func (c *client) unsubscribeAll() {
	// Snapshot to avoid lock inversion (client -> queue/topic)
	c.subsMu.Lock()
	handlers := make([]handler, 0, len(c.subs))
	for k, h := range c.subs {
		handlers = append(handlers, h)
		delete(c.subs, k)
	}
	c.subsMu.Unlock()

	for _, h := range handlers {
		if h != nil {
			h.remove() // O(1), thread-safe via DList’s internal lock
		}
	}
}
