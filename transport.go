package socketio2

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// buildWebsocketURL derives the Engine.IO v3 websocket handshake URL and the
// Socket.IO namespace from a socket.io-client v2 style host, e.g.
// "https://example.com/my-namespace" -> namespace "/my-namespace", url
// "wss://example.com/socket.io/?EIO=3&transport=websocket&...".
//
// This mirrors how socket.io-client v2 treats `io.connect(host, opts)`: the
// URL path becomes the namespace, and the actual Engine.IO handshake always
// happens against opts.Path (default "/socket.io/").
func buildWebsocketURL(opts Options) (wsURL, nsp string, err error) {
	u, err := url.Parse(opts.Host)
	if err != nil {
		return "", "", fmt.Errorf("socketio2: parse host %q: %w", opts.Host, err)
	}

	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	case "ws", "wss":
	default:
		return "", "", fmt.Errorf("socketio2: unsupported scheme %q in host %q", u.Scheme, opts.Host)
	}

	nsp = u.Path
	if nsp == "" {
		nsp = "/"
	}
	u.Path = opts.enginePath()

	q := url.Values{}
	q.Set("EIO", "3")
	q.Set("transport", "websocket")
	for k, v := range opts.Query {
		q.Set(k, v)
	}
	u.RawQuery = q.Encode()

	return u.String(), nsp, nil
}

// transport wraps a websocket connection dedicated to the Engine.IO v3
// "websocket-only" transport: every websocket frame is exactly one
// Engine.IO packet (no polling-style batching/base64 framing involved).
// Writes are serialized because gorilla/websocket only supports one
// concurrent writer per connection.
type transport struct {
	conn    *websocket.Conn
	writeMu sync.Mutex
}

// dial connects to wsURL and consumes the Engine.IO OPEN packet the server
// sends immediately after the websocket handshake.
func dial(ctx context.Context, wsURL string, handshakeTimeout time.Duration) (*transport, openPacket, error) {
	dialer := websocket.Dialer{HandshakeTimeout: handshakeTimeout}
	conn, _, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return nil, openPacket{}, fmt.Errorf("socketio2: dial websocket: %w", err)
	}

	t := &transport{conn: conn}

	eType, payload, err := t.readPacket()
	if err != nil {
		_ = conn.Close()
		return nil, openPacket{}, fmt.Errorf("socketio2: read open packet: %w", err)
	}
	if eType != eioOpen {
		_ = conn.Close()
		return nil, openPacket{}, fmt.Errorf("socketio2: expected engine.io OPEN packet, got type %q", byte(eType))
	}

	var open openPacket
	if err := json.Unmarshal([]byte(payload), &open); err != nil {
		_ = conn.Close()
		return nil, openPacket{}, fmt.Errorf("socketio2: decode open packet: %w", err)
	}

	return t, open, nil
}

func (t *transport) readPacket() (eioType, string, error) {
	_, data, err := t.conn.ReadMessage()
	if err != nil {
		return 0, "", err
	}
	return decodeEIOPacket(string(data))
}

func (t *transport) writePacket(typ eioType, payload string) error {
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	return t.conn.WriteMessage(websocket.TextMessage, []byte(encodeEIOPacket(typ, payload)))
}

func (t *transport) setReadDeadline(deadline time.Time) error {
	return t.conn.SetReadDeadline(deadline)
}

func (t *transport) close() error {
	return t.conn.Close()
}
