package minibus

import (
	"context"
	"sync"
)

type consumer struct {
	queue  *queue
	client *client
	name   string
	once   bool
	node   *node[*consumer]
}

func (c *consumer) remove() {
	// Safe & idempotent: node.remove() locks the DList internally.
	if n := c.node; n != nil {
		n.remove()
		c.node = nil
	}
}

type queue struct {
	name        string
	messages    chan message
	consumersMu sync.Mutex
	consumers   DList[*consumer] // round robin list
	closedMu    sync.Mutex
	closed      bool
}

func newQueue(name string, buf int) *queue {
	q := &queue{
		name:     name,
		messages: make(chan message, buf),
	}
	go q.run()
	return q
}

func (q *queue) isClosed() bool {
	q.closedMu.Lock()
	closed := q.closed
	q.closedMu.Unlock()
	return closed
}

func (q *queue) close() {
	q.closedMu.Lock()
	if q.closed {
		q.closedMu.Unlock()
		return
	}
	q.closed = true
	q.closedMu.Unlock()
	close(q.messages)
}

func (q *queue) run() {
	ctx := context.Background()
	for m := range q.messages {
		q.dispatch(ctx, m)
	}
}

func (q *queue) addConsumer(cl *client, name string, once bool) *consumer {
	c := &consumer{queue: q, client: cl, name: name, once: once}
	c.node = q.consumers.push(c)
	return c
}

func (q *queue) enqueue(m message) bool {
	if q.isClosed() {
		return false
	}
	select {
	case q.messages <- m:
		return true
	default:
		return false // backpressure policy: drop on full (minimal)
	}
}

func (q *queue) dispatch(ctx context.Context, msg message) {
	// Take the front node
	n := q.consumers.pop()
	if n == nil {
		return
	}
	c := n.value

	// persist or detach
	if !c.once {
		q.consumers.append(n)
	} else {
		// one-shot: leave detached; client map cleanup after successful send
	}

	// Deliver (no list locks held)
	if err := c.client.writeJSON(ctx, Frame{
		What: "msg",
		From: q.name,
		To:   c.name,
		Id:   msg.id,
		Data: msg.data,
	}); err != nil {
		if c.node != nil {
			c.node.remove()
		}
		c.client.unsubscribe(q.name)
		return
	}

	// If it was one-shot, drop from the client’s map after successful send
	if c.once {
		c.client.unsubscribe(q.name)
	}
}
