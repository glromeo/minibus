package minibus

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// Queue-specific tests

func TestDefaultKind_Queue(t *testing.T) {
	srv, _ := startTestServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	con := dialWS(t, ctx, wsURL(t, srv.URL, "/minibus"))
	// Subscribe without specifying Kind; server should treat as queue
	mustWrite(t, ctx, con, Frame{Type: "sub", Target: "qk"})

	pub := dialWS(t, ctx, wsURL(t, srv.URL, "/minibus"))
	payload, _ := json.Marshal("kdefault")
	mustWrite(t, ctx, pub, Frame{Type: "pub", Target: "qk", ID: "kid", Data: payload})

	var ack Frame
	mustRead(t, ctx, pub, &ack)
	if ack.Type != "ack" || ack.ID != "kid" || ack.Kind != QUEUE {
		t.Fatalf("expected ack with Kind=queue, got %#v", ack)
	}

	var msg Frame
	mustRead(t, ctx, con, &msg)
	if msg.Type != "msg" || msg.Kind != QUEUE || msg.Target != "qk" || msg.ID != "kid" || string(msg.Data) != `"kdefault"` {
		t.Fatalf("unexpected message: %#v", msg)
	}
}

func TestPubSub_Basic(t *testing.T) {
	srv, cfg := startTestServer(t)
	_ = cfg

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Consumer subscribes to q1
	con := dialWS(t, ctx, wsURL(t, srv.URL, "/minibus"))
	mustWrite(t, ctx, con, Frame{Type: "sub", Target: "q1"})

	// Supplier publishes "hello"
	pub := dialWS(t, ctx, wsURL(t, srv.URL, "/minibus"))
	payload, _ := json.Marshal("hello")
	mustWrite(t, ctx, pub, Frame{Type: "pub", Target: "q1", ID: "1", Data: payload})

	// Expect ack to publisher
	var ack Frame
	mustRead(t, ctx, pub, &ack)
	if ack.Type != "ack" || ack.Target != "q1" || ack.ID != "1" {
		t.Fatalf("expected ack for q1/1, got %#v", ack)
	}

	// Expect delivery to consumer
	var msg Frame
	mustRead(t, ctx, con, &msg)
	if msg.Type != "msg" || msg.Target != "q1" || msg.ID != "1" || string(msg.Data) != `"hello"` {
		t.Fatalf(`expected msg "hello" on q1/1, got %#v`, msg)
	}
}

func TestSubscribe_Once(t *testing.T) {
	srv, _ := startTestServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	con := dialWS(t, ctx, wsURL(t, srv.URL, "/minibus"))
	// one-shot subscribe
	mustWrite(t, ctx, con, Frame{Type: "once", Target: "q2"})

	pub := dialWS(t, ctx, wsURL(t, srv.URL, "/minibus"))

	// Publish two messages
	payload1, _ := json.Marshal("first")
	mustWrite(t, ctx, pub, Frame{Type: "pub", Target: "q2", ID: "m1", Data: payload1})
	var ack Frame
	mustRead(t, ctx, pub, &ack)
	if ack.Type != "ack" || ack.ID != "m1" {
		t.Fatalf("expected ack for m1, got %#v", ack)
	}

	var msg Frame
	mustRead(t, ctx, con, &msg)
	if msg.Type != "msg" || msg.ID != "m1" || string(msg.Data) != `"first"` {
		t.Fatalf("expected one-shot delivery of m1, got %#v", msg)
	}

	// Second publish should NOT arrive (one-shot already removed)
	payload2, _ := json.Marshal("second")
	mustWrite(t, ctx, pub, Frame{Type: "pub", Target: "q2", ID: "m2", Data: payload2})
	mustRead(t, ctx, pub, &ack)
	if ack.Type != "ack" || ack.ID != "m2" {
		t.Fatalf("expected ack for m2, got %#v", ack)
	}
	// No message should be readable within a short timeout
	if ok := readWithTimeout(t, 150*time.Millisecond, con, &msg); ok {
		t.Fatalf("unexpected second delivery to one-shot subscriber: %#v", msg)
	}
}

func TestRoundRobin_TwoConsumers(t *testing.T) {
	srv, _ := startTestServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conA := dialWS(t, ctx, wsURL(t, srv.URL, "/minibus"))
	conB := dialWS(t, ctx, wsURL(t, srv.URL, "/minibus"))
	mustWrite(t, ctx, conA, Frame{Type: "sub", Target: "q3"})
	mustWrite(t, ctx, conB, Frame{Type: "sub", Target: "q3"})

	pub := dialWS(t, ctx, wsURL(t, srv.URL, "/minibus"))

	// Publish 4 messages with IDs "1".."4"
	for i := 1; i <= 4; i++ {
		payload, _ := json.Marshal(i)
		mustWrite(t, ctx, pub, Frame{
			Type:   "pub",
			Target: "q3",
			ID:     strconv.Itoa(i),
			Data:   payload,
		})
		var ack Frame
		mustRead(t, ctx, pub, &ack)
		if ack.Type != "ack" || ack.ID != strconv.Itoa(i) {
			t.Fatalf("expected ack for %d, got %#v", i, ack)
		}
	}

	// Expect alternating deliveries: A gets 1,3 and B gets 2,4
	readID := func(c *websocket.Conn) string {
		var f Frame
		mustRead(t, ctx, c, &f)
		return f.ID
	}
	a1 := readID(conA)
	b1 := readID(conB)
	a2 := readID(conA)
	b2 := readID(conB)

	if a1 != "1" || a2 != "3" || b1 != "2" || b2 != "4" {
		t.Fatalf("round-robin mismatch: A=[%s %s] B=[%s %s]", a1, a2, b1, b2)
	}
}

func TestDisconnectCleanup_Resubscribe(t *testing.T) {
	srv, _ := startTestServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// First consumer subscribes then disconnects
	con1 := dialWS(t, ctx, wsURL(t, srv.URL, "/minibus"))
	mustWrite(t, ctx, con1, Frame{Type: "sub", Target: "q4"})

	// Now close it (simulating abrupt client departure)
	con1.Close(websocket.StatusNormalClosure, "")

	// Publish after disconnect; should not block the server and should ack
	pub := dialWS(t, ctx, wsURL(t, srv.URL, "/minibus"))
	payload, _ := json.Marshal("after-close")
	mustWrite(t, ctx, pub, Frame{Type: "pub", Target: "q4", ID: "a1", Data: payload})
	var ack Frame
	mustRead(t, ctx, pub, &ack)
	if ack.Type != "ack" || ack.ID != "a1" {
		t.Fatalf("expected ack after consumer disconnect, got %#v", ack)
	}

	// New consumer subscribes and should receive subsequent messages
	con2 := dialWS(t, ctx, wsURL(t, srv.URL, "/minibus"))
	mustWrite(t, ctx, con2, Frame{Type: "sub", Target: "q4"})

	payload2, _ := json.Marshal("hi again")
	mustWrite(t, ctx, pub, Frame{Type: "pub", Target: "q4", ID: "a2", Data: payload2})
	mustRead(t, ctx, pub, &ack)
	if ack.Type != "ack" || ack.ID != "a2" {
		t.Fatalf("expected ack for a2, got %#v", ack)
	}

	var msg Frame
	mustRead(t, ctx, con2, &msg)
	if msg.Type != "msg" || msg.ID != "a2" || string(msg.Data) != `"hi again"` {
		t.Fatalf("expected delivery to new consumer, got %#v", msg)
	}
}
