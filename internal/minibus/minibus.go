package minibus

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

type Kind string

type Frame struct {
	Type   string          `json:"type"`             // pub|sub|msg|ack|nack|flow|ping|pong|once
	Kind   Kind            `json:"kind,omitempty"`   // "queue" | "topic"
	Target string          `json:"target,omitempty"` // queue name
	ID     string          `json:"id,omitempty"`
	Data   json.RawMessage `json:"data,omitempty"`
}

const (
	TOPIC Kind = "topic"
	QUEUE      = "queue"
)

type message struct {
	id   string
	data json.RawMessage
}

func handleWebSocket(config Config) http.HandlerFunc {

	hub := newHub(config)

	return func(writer http.ResponseWriter, request *http.Request) {
		ws, err := websocket.Accept(writer, request, &config.Accept)
		if err != nil {
			http.Error(writer, "upgrade failed", http.StatusBadRequest)
			return
		}
		defer ws.Close(websocket.StatusNormalClosure, "")

		ctx := request.Context()
		ws.SetReadLimit(config.ReadLimit)

		client := &client{
			ws:   ws,
			hub:  hub,
			subs: make(map[string]handler),
		}

		for {
			var f Frame
			if err := wsjson.Read(ctx, ws, &f); err != nil {
				client.unsubscribeAll()
				return
			}

			// default to queue kind when not provided, matching protocol expectations
			if f.Kind == "" {
				f.Kind = QUEUE
			}

			switch f.Type {
			case "ping":
				_ = client.writeJSON(ctx, Frame{Type: "pong"})
			case "once", "sub":
				if f.Target == "" {
					continue
				}
				client.subscribe(f.Kind, f.Target, f.Type == "once")
			case "pub":
				if f.Target == "" || len(f.Data) == 0 {
					continue
				}
				msg := message{id: f.ID, data: f.Data}
				var ok bool
				if f.Kind == "queue" {
					ok = hub.getQueue(f.Target).enqueue(msg)
				} else {
					ok = hub.getTopic(f.Target).publish(msg)
				}
				if !ok {
					// Optional: tell publisher we dropped due to backpressure
					_ = client.writeJSON(ctx, Frame{
						Type:   "nack",
						Kind:   f.Kind,
						Target: f.Target,
						ID:     f.ID,
						Data:   json.RawMessage(`"queue full"`),
					})
				} else {
					// Optional: confirm
					_ = client.writeJSON(ctx, Frame{
						Type:   "ack",
						Kind:   f.Kind,
						Target: f.Target,
						ID:     f.ID,
					})
				}
			case "ack", "nack", "flow":
			default:
				m, _ := json.Marshal(f)
				log.Printf("ignored frame %s\n", m)
			}
		}
	}
}

func Run(options ...Option) error {
	config := defaultConfig()
	for _, option := range options {
		option(&config)
	}
	http.HandleFunc("/minibus", handleWebSocket(config))
	addr := "0.0.0.0:" + config.Port
	srv := &http.Server{
		Addr:              addr,
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Printf("listening on %s\n", addr)
	return srv.ListenAndServe()
}
