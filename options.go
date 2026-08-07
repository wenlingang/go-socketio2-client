package socketio2

import "time"

// Options configures a Client. Only Host is required; everything else has
// defaults matching socket.io-client v2's own defaults.
type Options struct {
	// Host is the server URL including the Socket.IO namespace as its URL
	// path, e.g. "https://example.com/my-namespace". This mirrors
	// socket.io-client v2's `io.connect(host, opts)` where the URL path is
	// treated as the namespace, not an HTTP path.
	Host string

	// Query holds extra query parameters merged into the Engine.IO
	// handshake URL (alongside the EIO/transport params this library sets
	// automatically), e.g. auth tokens the server expects on connect.
	Query map[string]string

	// Path is the Engine.IO handshake path. Defaults to "/socket.io/".
	Path string

	// DialTimeout bounds the websocket handshake. Defaults to 15s.
	DialTimeout time.Duration

	// WriteTimeout bounds a single packet write. Defaults to 10s. It stops a
	// peer that has stopped reading from blocking writes (and therefore every
	// concurrent Emit) indefinitely.
	WriteTimeout time.Duration

	// ReconnectionDelay is the initial delay before the first reconnect
	// attempt. Defaults to 1s, matching socket.io-client v2.
	ReconnectionDelay time.Duration

	// ReconnectionDelayMax caps the reconnect backoff. Defaults to 5s,
	// matching socket.io-client v2.
	ReconnectionDelayMax time.Duration

	// RandomizationFactor jitters the reconnect delay by +/- this fraction.
	// Defaults to 0.5, matching socket.io-client v2.
	RandomizationFactor float64

	// MaxReconnectAttempts caps the number of consecutive failed reconnect
	// attempts before Run gives up and returns. Zero (the default) means
	// unlimited retries, matching socket.io-client v2's default of Infinity.
	MaxReconnectAttempts int
}

func (o Options) enginePath() string {
	if o.Path != "" {
		return o.Path
	}
	return "/socket.io/"
}

func (o Options) dialTimeout() time.Duration {
	if o.DialTimeout > 0 {
		return o.DialTimeout
	}
	return 15 * time.Second
}

func (o Options) writeTimeout() time.Duration {
	if o.WriteTimeout > 0 {
		return o.WriteTimeout
	}
	return 10 * time.Second
}

func (o Options) reconnectionDelay() time.Duration {
	if o.ReconnectionDelay > 0 {
		return o.ReconnectionDelay
	}
	return time.Second
}

func (o Options) reconnectionDelayMax() time.Duration {
	if o.ReconnectionDelayMax > 0 {
		return o.ReconnectionDelayMax
	}
	return 5 * time.Second
}

func (o Options) randomizationFactor() float64 {
	if o.RandomizationFactor > 0 {
		return o.RandomizationFactor
	}
	return 0.5
}
