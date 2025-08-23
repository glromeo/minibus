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
	subs   map[string]handler
}

func (c *client) writeJSON(ctx context.Context, f any) error {
	c.wsMu.Lock()
	defer c.wsMu.Unlock()
	return wsjson.Write(ctx, c.ws, f)
}

func (c *client) subscribe(to, from string, once bool) {
	var h handler
	if to[0] == '#' {
		t := c.hub.getTopic(to)
		h = t.addSubscriber(c, from, once)
	} else {
		q := c.hub.getQueue(to)
		h = q.addConsumer(c, from, once)
	}

	// If already subscribed, remove old handle first (avoid duplicates/leaks)
	var old handler
	c.subsMu.Lock()
	if prev, ok := c.subs[to]; ok {
		old = prev
	}
	c.subs[to] = h
	c.subsMu.Unlock()

	if old != nil {
		old.remove()
	}
}

func (c *client) unsubscribe(name string) {
	// Take the handle without holding the lock during remove()
	c.subsMu.Lock()
	h := c.subs[name]
	delete(c.subs, name)
	c.subsMu.Unlock()

	if h != nil {
		h.remove()
	}
}

func (c *client) unsubscribeAll() {
	// Snapshot To avoid lock inversion (client -> queue/topic)
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
