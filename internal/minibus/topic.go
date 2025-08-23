package minibus

import "context"

type subscriber struct {
	topic  *topic
	client *client
	once   bool
	node   *node[*subscriber]
}

func (s *subscriber) remove() {
	// Safe & idempotent: node.remove() locks the DList internally.
	if n := s.node; n != nil {
		n.remove()
		s.node = nil
	}
}

type topic struct {
	name string
	msgs chan message
	subs DList[*subscriber] // self-locked list
}

func newTopic(name string, buf int) *topic {
	t := &topic{name: name, msgs: make(chan message, buf)}
	go t.run()
	return t
}

func (t *topic) addSubscriber(cl *client, once bool) *subscriber {
	s := &subscriber{topic: t, client: cl, once: once}
	s.node = t.subs.push(s)
	return s
}

func (t *topic) publish(m message) bool {
	select {
	case t.msgs <- m:
		return true
	default:
		return false // minimal backpressure: drop
	}
}

func (t *topic) run() {
	ctx := context.Background()
	for m := range t.msgs {
		t.dispatch(ctx, m)
	}
}

// fan-out to all current subs; remove one-shots; drop dead writers
func (t *topic) dispatch(ctx context.Context, msg message) {
	// Iterate by cycling the list: Pop each node once, Append back if persistent.
	// This preserves order and avoids an external iteration API.
	for {
		n := t.subs.pop()
		if n == nil {
			break
		} // no (more) subscribers
		s := n.value

		// persist or detach
		if !s.once {
			t.subs.append(n)
		} else {
			// one-shot: leave detached; client map cleanup after successful send
		}

		// deliver (no list locks held)
		if err := s.client.writeJSON(ctx, Frame{
			Type:   "msg",
			Kind:   TOPIC,
			Target: t.name,
			ID:     msg.id,
			Data:   msg.data,
		}); err != nil {
			// Writer dead: ensure it’s not in the list and remove from client map
			if s.node != nil {
				s.node.remove()
				s.node = nil
			}
			s.client.unsubscribe(TOPIC, t.name)
			continue
		}

		// If it was one-shot, drop from the client’s map after successful send
		if s.once {
			s.client.unsubscribe(TOPIC, t.name)
		}
	}
}
