# Minibus

A tiny in-memory WebSocket message bus for Go. Minibus exposes a single WS endpoint at `/minibus` and supports:
- Queues (default): work-queue semantics with round‑robin delivery to consumers
- Topics: pub/sub fanout to all active subscribers
- One-shot subscriptions (receive the next message then auto-unsubscribe)
- Simple backpressure (drops when buffers are full) with ack/nack to publishers

## Requirements
- Go 1.25+

## Get started
Install dependencies and run the server example:

```bash
# from repository root
go mod tidy

# run the example server (listens on :4060)
go run ./cmd/minibus
```

Connect a WebSocket client to: `ws://localhost:4060/minibus`.

Quick ping/pong check (JSON frames):
- Send: `{ "type": "ping" }`
- Receive: `{ "type": "pong" }`

### Queue example (default Kind = "queue")
- Consumer subscribes: `{ "type": "sub", "target": "jobs" }`
- Publisher sends: `{ "type": "pub", "target": "jobs", "id": "42", "data": "\"hello\"" }`
- Publisher receives: `{ "type": "ack", "id": "42", "kind": "queue", "target": "jobs" }`
- Consumer receives: `{ "type": "msg", "id": "42", "kind": "queue", "target": "jobs", "data": "\"hello\"" }`

### Topic example
Topic operations require `kind: "topic"`:
- Subscriber: `{ "type": "sub", "kind": "topic", "target": "news" }`
- Publisher: `{ "type": "pub", "kind": "topic", "target": "news", "id": "n1", "data": "\"update\"" }`
- All active subscribers to `news` receive the message.

## Programmatic usage
You can embed the server in your Go app.

```go
package main

import (
    "log"
    minibus "github.com/glromeo/minibus/internal/minibus"
)

func main() {
    if err := minibus.Run(
        minibus.WithPort("4060"),            // listen port
        minibus.WithBufferSize(2048),         // queue/topic buffer size
        // minibus.WithAcceptOptions(...),    // customize websocket.AcceptOptions (origins, etc.)
    ); err != nil {
        log.Fatal(err)
    }
}
```

## Building a binary
```bash
# produce a Windows-friendly example binary in .\bin
mkdir -p bin 2> NUL
go build -o .\bin\minibus .\cmd\minibus
```

## Testing
```bash
go test ./...
```

Notes
- The server defaults frame `kind` to `"queue"` when omitted (backward‑compatible clients).
- The default CORS/CSRF origins allow localhost; configure `WithAcceptOptions` for other deployments.