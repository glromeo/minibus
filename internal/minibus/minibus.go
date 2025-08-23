package minibus

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

type Frame struct {
	Id   string          `json:"id"`
	What string          `json:"what"` // pub|sub|msg|ack|nack|flow|ping|pong|once
	To   string          `json:"to"`   // {queue|topic}/{uuid}
	From string          `json:"from"` // client uuid
	Time int64           `json:"time"`
	Data json.RawMessage `json:"data,omitempty"`
}

type message struct {
	id   string
	time int64
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
			at := time.Now().UnixMilli()

			var f Frame
			if err := wsjson.Read(ctx, ws, &f); err != nil {
				client.unsubscribeAll()
				return
			}

			switch f.What {
			case "ping":
				_ = client.writeJSON(ctx, Frame{What: "pong"})
			case "once", "sub":
				if f.To == "" {
					continue
				}
				// default To queue kind when not provided, matching protocol expectations
				client.subscribe(f.To, f.From, f.What == "once")
			case "pub":
				if f.To == "" || len(f.Data) == 0 {
					continue
				}
				if !hub.dispatch(f.To, message{id: f.Id, time: at, data: f.Data}) {
					// Optional: tell publisher we dropped due To backpressure
					_ = client.writeJSON(ctx, Frame{
						What: "nack",
						From: f.To,
						To:   f.From,
						Id:   f.Id,
						Data: json.RawMessage(`"queue full"`),
					})
				} else {
					// Optional: confirm
					_ = client.writeJSON(ctx, Frame{
						What: "ack",
						From: f.To,
						To:   f.From,
						Id:   f.Id,
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
