package minibus

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// Topic-specific tests

func TestTopic_Once(t *testing.T) {
	wsURL, _ := startTestServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	con := dialWS(t, ctx, wsURL)
	tn := "#t0"
	mustWrite(t, ctx, con, Frame{What: "once", To: tn})

	pub := dialWS(t, ctx, wsURL)
	p1, _ := json.Marshal("first")
	mustWrite(t, ctx, pub, Frame{What: "pub", To: tn, Id: "x1", Data: p1})
	var ack Frame
	mustRead(t, ctx, pub, &ack)
	if ack.What != "ack" || ack.From != tn || ack.Id != "x1" {
		t.Fatalf("expected topic ack for x1, got %#v", ack)
	}
	var m Frame
	mustRead(t, ctx, con, &m)
	if m.What != "msg" || m.From != tn || m.Id != "x1" || string(m.Data) != `"first"` {
		t.Fatalf("unexpected first message: %#v", m)
	}

	// second publish should not be delivered To one-shot sub
	p2, _ := json.Marshal("second")
	mustWrite(t, ctx, pub, Frame{What: "pub", To: tn, Id: "x2", Data: p2})
	mustRead(t, ctx, pub, &ack)
	if ack.What != "ack" || ack.From != tn || ack.Id != "x2" {
		t.Fatalf("expected topic ack for x2, got %#v", ack)
	}
	if ok := readWithTimeout(t, 150*time.Millisecond, con, &m); ok {
		t.Fatalf("unexpected second delivery: %#v", m)
	}
}

func TestTopic_Fanout_Basic(t *testing.T) {
	wsURL, _ := startTestServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Two subscribers To the same topic
	conA := dialWS(t, ctx, wsURL)
	conB := dialWS(t, ctx, wsURL)
	tn := "#t1"
	from := "0a5f3c82-2d4e-4f1b-91a6-8b9d7c2e5f31"
	mustWrite(t, ctx, conA, Frame{What: "sub", To: tn, From: from})
	mustWrite(t, ctx, conB, Frame{What: "sub", To: tn, From: from})

	// Publish one message To the topic
	pub := dialWS(t, ctx, wsURL)
	payload, _ := json.Marshal("broadcast")
	id := "d8f2b6a1-7c9e-45e1-82f0-3a4c5b6d7e28"
	mustWrite(t, ctx, pub, Frame{What: "pub", To: tn, From: from, Id: id, Data: payload})

	// Expect ack To publisher
	var ack Frame
	mustRead(t, ctx, pub, &ack)
	if ack.What != "ack" || ack.Id != id || ack.From != tn {
		t.Fatalf("expected topic ack tb1, got %#v", ack)
	}

	// Both subscribers should receive the message
	var mA, mB Frame
	mustRead(t, ctx, conA, &mA)
	mustRead(t, ctx, conB, &mB)
	if mA.What != "msg" || mA.From != tn || mA.To != from || mA.Id != id || string(mA.Data) != `"broadcast"` {
		t.Fatalf("A got unexpected message: %#v", mA)
	}
	if mB.What != "msg" || mB.From != tn || mB.To != from || mB.Id != id || string(mB.Data) != `"broadcast"` {
		t.Fatalf("B got unexpected message: %#v", mB)
	}
}
