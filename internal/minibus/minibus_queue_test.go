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

func TestQueue_Simple(t *testing.T) {
	wsURL, _ := startTestServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	con := dialWS(t, ctx, wsURL)
	qn := "@alpha"
	mustWrite(t, ctx, con, Frame{What: "sub", To: qn})

	pub := dialWS(t, ctx, wsURL)
	payload, _ := json.Marshal("sample data")
	msgId := "f47ac10b-58cc-4372-a567-0e02b2c3d479"
	mustWrite(t, ctx, pub, Frame{What: "pub", To: qn, Id: msgId, Data: payload})

	var ack Frame
	mustRead(t, ctx, pub, &ack)
	if ack.What != "ack" || ack.Id != msgId || ack.From != qn {
		t.Fatalf("expected ack from queue, got %#v", ack)
	}

	var msg Frame
	mustRead(t, ctx, con, &msg)
	if msg.What != "msg" || msg.From != qn || msg.Id != msgId || string(msg.Data) != `"sample data"` {
		t.Fatalf("unexpected message: %#v", msg)
	}
}

func TestPubSub_Basic(t *testing.T) {
	wsURL, _ := startTestServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Consumer subscribes To q1
	con := dialWS(t, ctx, wsURL)
	qn := "@q1"
	mustWrite(t, ctx, con, Frame{What: "sub", To: qn})

	// Supplier publishes "hello"
	pub := dialWS(t, ctx, wsURL)
	payload, _ := json.Marshal("hello")
	id := "550e8400-e29b-41d4-a716-446655440000"
	mustWrite(t, ctx, pub, Frame{What: "pub", To: qn, Id: id, Data: payload})

	// Expect ack To publisher
	var ack Frame
	mustRead(t, ctx, pub, &ack)
	if ack.What != "ack" || ack.From != qn || ack.Id != id {
		t.Fatalf("expected ack for @q1, got %#v", ack)
	}

	// Expect delivery To consumer
	var msg Frame
	mustRead(t, ctx, con, &msg)
	if msg.What != "msg" || msg.From != qn || msg.Id != id || string(msg.Data) != `"hello"` {
		t.Fatalf(`expected msg "hello" on @q1, got %#v`, msg)
	}
}

func TestSubscribe_Once(t *testing.T) {
	wsURL, _ := startTestServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	con := dialWS(t, ctx, wsURL)
	// one-shot subscribe
	qn := "@q2"
	mustWrite(t, ctx, con, Frame{What: "once", To: qn})

	pub := dialWS(t, ctx, wsURL)

	// Publish two messages
	payload1, _ := json.Marshal("first")
	id1 := "9b2c4f10-2a6e-4c3f-8d7e-1f2a5e6c8b9d"
	mustWrite(t, ctx, pub, Frame{What: "pub", To: qn, Id: id1, Data: payload1})
	var ack Frame
	mustRead(t, ctx, pub, &ack)
	if ack.What != "ack" || ack.Id != id1 {
		t.Fatalf("expected ack for m1, got %#v", ack)
	}

	var msg Frame
	mustRead(t, ctx, con, &msg)
	if msg.What != "msg" || msg.Id != id1 || string(msg.Data) != `"first"` {
		t.Fatalf("expected one-shot delivery of 9b2c4f10-2a6e-4c3f-8d7e-1f2a5e6c8b9d, got %#v", msg)
	}

	// Second publish should NOT arrive (one-shot already removed)
	payload2, _ := json.Marshal("second")
	id2 := "1f9d2a64-3e2b-4b8a-9d5f-7c8b1a2e3f4d"
	mustWrite(t, ctx, pub, Frame{What: "pub", To: qn, Id: id2, Data: payload2})
	mustRead(t, ctx, pub, &ack)
	if ack.What != "ack" || ack.Id != id2 {
		t.Fatalf("expected ack for 1f9d2a64-3e2b-4b8a-9d5f-7c8b1a2e3f4d, got %#v", ack)
	}
	// No message should be readable within a short timeout
	if ok := readWithTimeout(t, 150*time.Millisecond, con, &msg); ok {
		t.Fatalf("unexpected second delivery To one-shot subscriber: %#v", msg)
	}
}

func TestRoundRobin_TwoConsumers(t *testing.T) {
	wsURL, _ := startTestServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conA := dialWS(t, ctx, wsURL)
	conB := dialWS(t, ctx, wsURL)
	qn := "@q3"
	mustWrite(t, ctx, conA, Frame{What: "sub", To: qn})
	mustWrite(t, ctx, conB, Frame{What: "sub", To: qn})

	pub := dialWS(t, ctx, wsURL)

	// Publish 4 messages with IDs "1".."4"
	for i := 1; i <= 4; i++ {
		payload, _ := json.Marshal(i)
		mustWrite(t, ctx, pub, Frame{
			Id:   strconv.Itoa(i),
			What: "pub",
			To:   qn,
			Data: payload,
		})
		var ack Frame
		mustRead(t, ctx, pub, &ack)
		if ack.What != "ack" || ack.Id != strconv.Itoa(i) {
			t.Fatalf("expected ack for %d, got %#v", i, ack)
		}
	}

	// Expect alternating deliveries: A gets 1,3 and B gets 2,4
	readID := func(c *websocket.Conn) string {
		var f Frame
		mustRead(t, ctx, c, &f)
		return f.Id
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
	wsURL, _ := startTestServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// First consumer subscribes then disconnects
	con1 := dialWS(t, ctx, wsURL)
	qn := "@q4"
	mustWrite(t, ctx, con1, Frame{What: "sub", To: qn})

	// Now close it (simulating abrupt client departure)
	con1.Close(websocket.StatusNormalClosure, "")

	// Publish after disconnect; should not block the server and should ack
	pub := dialWS(t, ctx, wsURL)
	payload, _ := json.Marshal("after-close")
	id1 := "6c4a9b20-1d7f-4e3b-8f29-9a7d6c2b5e10"
	mustWrite(t, ctx, pub, Frame{What: "pub", To: qn, Id: id1, Data: payload})
	var ack Frame
	mustRead(t, ctx, pub, &ack)
	if ack.What != "ack" || ack.Id != id1 {
		t.Fatalf("expected ack after consumer disconnect, got %#v", ack)
	}

	// New consumer subscribes and should receive subsequent messages
	con2 := dialWS(t, ctx, wsURL)
	mustWrite(t, ctx, con2, Frame{What: "sub", To: qn})

	payload2, _ := json.Marshal("hi again")
	id2 := "b73e9f42-8c2d-41f6-93a7-2e4b8d6c5a19"
	mustWrite(t, ctx, pub, Frame{What: "pub", To: qn, Id: id2, Data: payload2})
	mustRead(t, ctx, pub, &ack)
	if ack.What != "ack" || ack.Id != id2 {
		t.Fatalf("expected ack for b73e9f42-8c2d-41f6-93a7-2e4b8d6c5a19, got %#v", ack)
	}

	var msg Frame
	mustRead(t, ctx, con2, &msg)
	if msg.What != "msg" || msg.Id != id2 || string(msg.Data) != `"hi again"` {
		t.Fatalf("expected delivery To new consumer, got %#v", msg)
	}
}
