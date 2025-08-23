package minibus

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

func wsURL(t *testing.T, base, path string) string {
	t.Helper()
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	u.Scheme = "ws"
	u.Path = path
	return u.String()
}

func startTestServer(t *testing.T) (srv *httptest.Server, cfg Config) {
	// default server with defaultConfig
	t.Helper()
	cfg = defaultConfig()
	return startTestServerWith(t, cfg)
}

// startTestServerWith allows customizing config (e.g., buffer sizes) per test
func startTestServerWith(t *testing.T, cfg Config) (srv *httptest.Server, outCfg Config) {
	t.Helper()
	outCfg = cfg
	mux := http.NewServeMux()
	mux.HandleFunc("/minibus", handleWebSocket(cfg))
	srv = httptest.NewServer(mux)
	t.Cleanup(func() { srv.Close() })
	return srv, outCfg
}

func dialWS(t *testing.T, ctx context.Context, wsAddr string) *websocket.Conn {
	t.Helper()
	c, _, err := websocket.Dial(ctx, wsAddr, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { c.Close(websocket.StatusNormalClosure, "") })
	return c
}

func mustWrite(t *testing.T, ctx context.Context, c *websocket.Conn, f Frame) {
	t.Helper()
	if err := wsjson.Write(ctx, c, f); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func mustRead(t *testing.T, ctx context.Context, c *websocket.Conn, out any) {
	t.Helper()
	if err := wsjson.Read(ctx, c, out); err != nil {
		t.Fatalf("read: %v", err)
	}
}

func readWithTimeout(t *testing.T, d time.Duration, c *websocket.Conn, out any) (ok bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	if err := wsjson.Read(ctx, c, out); err != nil {
		// expect deadline exceeded if nothing arrives
		return false
	}
	return true
}

// --- Tests ---

func TestDList_Basics(t *testing.T) {
	var l DList[int]
	// push a few, pop, append back, remove idempotently
	n1 := l.push(1)
	n2 := l.push(2)
	n3 := l.push(3)
	if n1 == nil || n2 == nil || n3 == nil {
		t.Fatal("push returned nil node")
	}
	// pop should return 1
	p1 := l.pop()
	if p1 == nil || p1.value != 1 {
		t.Fatalf("expected pop 1, got %#v", p1)
	}
	// append detached node back makes it tail: list order now 2,3,1
	l.append(p1)
	p2 := l.pop()
	p3 := l.pop()
	p4 := l.pop()
	if p2.value != 2 || p3.value != 3 || p4.value != 1 {
		t.Fatalf("unexpected order: %d %d %d", p2.value, p3.value, p4.value)
	}
	// removing detached nodes should be safe (idempotent)
	p4.remove()
	p4.remove()
	// list should be empty now
	if l.pop() != nil {
		t.Fatal("expected empty list")
	}
}

func TestHub_GetQueueTopic_Singleton(t *testing.T) {
	cfg := defaultConfig()
	h := newHub(cfg)
	q1 := h.getQueue("alpha")
	q2 := h.getQueue("alpha")
	if q1 == nil || q2 == nil || q1 != q2 {
		t.Fatal("expected same queue instance for same name")
	}
	t1 := h.getTopic("broadcast")
	t2 := h.getTopic("broadcast")
	if t1 == nil || t2 == nil || t1 != t2 {
		t.Fatal("expected same topic instance for same name")
	}
}

func TestPingPong(t *testing.T) {
	srv, _ := startTestServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	c := dialWS(t, ctx, wsURL(t, srv.URL, "/minibus"))
	mustWrite(t, ctx, c, Frame{Type: "ping"})

	var f Frame
	mustRead(t, ctx, c, &f)
	if f.Type != "pong" {
		t.Fatalf("expected pong, got %#v", f)
	}
}
