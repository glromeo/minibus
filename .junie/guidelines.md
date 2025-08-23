Project: Minibus (in-memory WS message bus)

Audience: Advanced Go developers working on this repository.

Build and Configuration
- Toolchain: Go 1.25 (as declared in go.mod). Use a recent Go toolchain; the code uses standard library + github.com/coder/websocket v1.8.13.
- Modules: go mod tidy will ensure dependencies (websocket) are present.
- Building the server binary:
  - Build the example CLI entrypoint: go build -o .\bin\minibus .\cmd\minibus
  - The server exposes a WebSocket endpoint at /minibus.
  - Default listen address via Run(defaultConfig()) is 0.0.0.0:4000. The cmd/minibus uses WithPort("4060").
- Running from code:
  - Programmatic: call internal/minibus.Run(...) with Option(s):
    - WithPort("<port>")
    - WithBufferSize(<int>) — per-queue/topic channel buffer and hub defaults.
    - WithAcceptOptions(websocket.AcceptOptions) — OriginPatterns, compression, etc. Note: default AcceptOptions include a restricted set of localhost origins; adjust to your deployment.
- Protocol summary (Frames):
  - Frame fields: Type (pub|sub|once|msg|ack|nack|flow|ping|pong), Kind ("queue"|"topic"), Target, ID, Data (json.RawMessage).
  - Defaulting: If Kind is omitted by the client, the server defaults it to "queue". This is important for backward compatibility with tests and simple clients.
  - Endpoints: Only WS on /minibus. No HTTP REST.

Runtime Semantics
- Queues vs Topics:
  - queue: work-queue semantics. Subscribers (consumers) receive messages round-robin; each message goes to exactly one consumer.
  - topic: pub-sub fanout semantics. Every active subscriber receives each message.
- Backpressure:
  - Minimal policy: enqueue/publish drops when the buffered channel is full (non-blocking select default case). Publishers receive ack on success, nack on drop with Data set to "queue full".
- One-shot subscription (Type: "once"):
  - The server delivers the first subsequent message, then detaches the handler and removes it from the client map after successful send.
- Ping/Pong:
  - Type: "ping" returns a "pong".

Concurrency Architecture (internal notes)
- DList[T]: a small mutex-protected doubly-linked list with O(1) push/pop/append/remove. Used to maintain subscriber/consumer order while minimizing lock contention. Methods lock internally.
  - append: assumes node is detached; sets list; updates tail.
  - pop: removes head and returns a detached node.
  - remove: idempotent; safe to call multiple times; checks ownership via node.list.
- client.writeJSON: guarded by wsMu to serialize writes per connection (websocket not safe for concurrent writes).
- client.subs: map of active handlers keyed by kind:name; protected by subsMu. Unsubscribe operations copy out handlers first to avoid lock inversion with DList internals.
- queue:
  - messages: buffered chan message; enqueue is non-blocking drop on full.
  - consumers: round-robin via DList. dispatch pops head, re-appends if persistent.
  - close/isClosed guarded by closedMu; run loop drains messages and dispatches until closed.
- topic:
  - msgs: buffered chan message; publish non-blocking drop on full.
  - subs: DList; dispatch cycles through current set: pop, optionally re-append for persistent, deliver, and remove dead writers.

Testing
- Packages with tests: internal/minibus (see minibus_test.go). Tests spin up an httptest.Server with the WS handler and then use github.com/coder/websocket to interact with it.
- Running tests:
  - All packages: go test ./...
  - Single package verbose: go test -v ./internal/minibus
  - Filter by test name: go test -run TestPingPong ./internal/minibus
  - With race detector: go test -race ./internal/minibus
  - With coverage: go test -cover ./internal/minibus
- Adding new tests:
  - Prefer using the existing utilities from minibus_test.go:
    - startTestServer(t) — returns an httptest.Server with handleWebSocket(defaultConfig()).
    - wsURL(t, base, path) — builds ws:// URL from server URL.
    - dialWS(t, ctx, wsAddr) — dials a WS connection and auto-closes in t.Cleanup.
    - mustWrite / mustRead — thin wrappers over wsjson.Write/Read that fail the test on error.
  - Use contexts with timeouts to avoid flakes (see readWithTimeout for negative assertions).
  - Example pattern:
    - Create connections for publisher/consumer.
    - Write a Frame to subscribe or publish.
    - Expect ack/nack on publisher; expect msg on consumer.
  - Be explicit about Frame.Kind when testing topics. For queues, it’s acceptable to omit Kind thanks to the server defaulting to "queue".
- Example minimal test (validated during authoring):
  - A simple ping/pong round-trip using startTestServer, dialWS, and wsjson.Write/Read was executed locally and passed. You can model quick connectivity checks on that pattern.

Debugging / Dev Tips
- Logging: handleWebSocket logs unknown frames at default level. Extend with additional logs around dispatch paths if needed.
- Origins (CORS/CSRF): Config.Accept.OriginPatterns defaults to various localhost patterns. Expand for your environment if you expose the server in browsers; otherwise, set AcceptOptions appropriately when calling Run.
- Buffer sizing: Config.BufferSize controls both queue/topic channel sizes and indirectly affects drop behavior. Large buffers reduce drops but increase memory and tail latency upon surges. Adjust in tests via a custom Config if you want deterministic drops.
- Determinism in tests:
  - Avoid time.Sleep — use ctx with deadlines and readWithTimeout for negative expectations.
  - Avoid relying on goroutine scheduling; the DList and per-connection write serialization should ensure ordering (round-robin for queues, broadcast for topics).
- API Stability Notes:
  - Defaulting Kind to queue is intentional to support older/simple clients and tests that don’t set Kind. If you add new frame types or change defaults, update tests and this guideline.

Build/Run Quickstart
- Install deps: go mod tidy
- Run tests: go test ./...
- Run the server locally:
  - go run ./cmd/minibus
  - Connect a WS client to ws://localhost:4060/minibus
  - Send {"type":"ping"} and expect {"type":"pong"}
  - Queue example:
    - Consumer: {"type":"sub","target":"jobs"}
    - Publisher: {"type":"pub","target":"jobs","id":"42","data":"\"hello\""}
    - Publisher gets ack; consumer gets msg with id 42 and data "hello".

Misc
- Clients:
  - Java client skeleton present in clients/java; not part of the core server build. Keep in sync with protocol defaults if you evolve frames.
  - JS client stub present; ensure CORS origins match if running from a browser.
- Repo layout:
  - internal/minibus — core library and tests.
  - cmd/minibus — example server entrypoint.

Changelog (recent internal note):
- The server now defaults Frame.Kind to "queue" when omitted by the client (inside handleWebSocket loop) to align with expected queue semantics in existing tests and examples. This fixes round-robin tests where missing Kind previously routed to topic fanout.
