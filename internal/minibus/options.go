package minibus

import "github.com/coder/websocket"

type Option func(*Config)

type Config struct {
	Port       string
	BufferSize int
	ReadLimit  int64
	Accept     websocket.AcceptOptions
}

func defaultConfig() Config {
	return Config{
		Port:       "4000",
		BufferSize: 1024,
		ReadLimit:  1 << 20,
		Accept: websocket.AcceptOptions{
			// put real origins here, not "*" if you care about CSRF:
			OriginPatterns: []string{
				"http://localhost", "https://localhost",
				"http://127.0.0.1", "https://127.0.0.1",
				"http://[::1]", "https://[::1]",
			},
		},
	}
}

func WithPort(addr string) Option {
	return func(c *Config) { c.Port = addr }
}

func WithBufferSize(size int) Option {
	return func(c *Config) { c.BufferSize = size }
}

func WithAcceptOptions(ao websocket.AcceptOptions) Option {
	// defensive copy avoids caller mutating shared slices later
	return func(c *Config) { c.Accept = ao }
}
