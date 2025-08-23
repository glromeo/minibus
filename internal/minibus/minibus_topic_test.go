package minibus

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// Topic-specific tests

func TestTopic_Once(t *testing.T) {
	srv, _ := startTestServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	con := dialWS(t, ctx, wsURL(t, srv.URL, "/minibus"))
	mustWrite(t, ctx, con, Frame{Type: "once", Kind: TOPIC, Target: "t_once"})

	pub := dialWS(t, ctx, wsURL(t, srv.URL, "/minibus"))
	p1, _ := json.Marshal("first")
	mustWrite(t, ctx, pub, Frame{Type: "pub", Kind: TOPIC, Target: "t_once", ID: "x1", Data: p1})
	var ack Frame
	mustRead(t, ctx, pub, &ack)
	if ack.Type != "ack" || ack.Kind != TOPIC || ack.ID != "x1" {
		t.Fatalf("expected topic ack for x1, got %#v", ack)
	}
	var m Frame
	mustRead(t, ctx, con, &m)
	if m.Type != "msg" || m.Kind != TOPIC || m.ID != "x1" || string(m.Data) != `"first"` {
		t.Fatalf("unexpected first message: %#v", m)
	}

	// second publish should not be delivered to one-shot sub
	p2, _ := json.Marshal("second")
	mustWrite(t, ctx, pub, Frame{Type: "pub", Kind: TOPIC, Target: "t_once", ID: "x2", Data: p2})
	mustRead(t, ctx, pub, &ack)
	if ack.Type != "ack" || ack.Kind != TOPIC || ack.ID != "x2" {
		t.Fatalf("expected topic ack for x2, got %#v", ack)
	}
	if ok := readWithTimeout(t, 150*time.Millisecond, con, &m); ok {
		t.Fatalf("unexpected second delivery: %#v", m)
	}
}

func TestTopic_Fanout_Basic(t *testing.T) {
	srv, _ := startTestServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Two subscribers to the same topic
	conA := dialWS(t, ctx, wsURL(t, srv.URL, "/minibus"))
	conB := dialWS(t, ctx, wsURL(t, srv.URL, "/minibus"))
	mustWrite(t, ctx, conA, Frame{Type: "sub", Kind: TOPIC, Target: "t1"})
	mustWrite(t, ctx, conB, Frame{Type: "sub", Kind: TOPIC, Target: "t1"})

	// Publish one message to the topic
	pub := dialWS(t, ctx, wsURL(t, srv.URL, "/minibus"))
	payload, _ := json.Marshal("broadcast")
	mustWrite(t, ctx, pub, Frame{Type: "pub", Kind: TOPIC, Target: "t1", ID: "tb1", Data: payload})

	// Expect ack to publisher
	var ack Frame
	mustRead(t, ctx, pub, &ack)
	if ack.Type != "ack" || ack.ID != "tb1" || ack.Kind != TOPIC {
		t.Fatalf("expected topic ack tb1, got %#v", ack)
	}

	// Both subscribers should receive the message
	var mA, mB Frame
	mustRead(t, ctx, conA, &mA)
	mustRead(t, ctx, conB, &mB)
	if mA.Type != "msg" || mA.Kind != TOPIC || mA.Target != "t1" || mA.ID != "tb1" || string(mA.Data) != `"broadcast"` {
		t.Fatalf("A got unexpected message: %#v", mA)
	}
	if mB.Type != "msg" || mB.Kind != TOPIC || mB.Target != "t1" || mB.ID != "tb1" || string(mB.Data) != `"broadcast"` {
		t.Fatalf("B got unexpected message: %#v", mB)
	}
}
