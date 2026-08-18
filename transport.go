package socketio2

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
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

// connectNamespace builds the namespace string for the Socket.IO CONNECT
// packet. socket.io-client v2 appends the socket-level query to the
// namespace before encoding it (socket.io-client/lib/manager.js:
// `if (packet.query && packet.type === 0) packet.nsp += '?' + packet.query`),
// so a server that reads credentials off the namespace query sees the same
// thing the JS client would have sent.
func connectNamespace(nsp string, query map[string]string) string {
	if len(query) == 0 || nsp == "/" {
		return nsp
	}
	q := url.Values{}
	for k, v := range query {
		q.Set(k, v)
	}
	return nsp + "?" + q.Encode()
}

// transport wraps a websocket connection dedicated to the Engine.IO v3
// "websocket-only" transport: every websocket frame is exactly one
// Engine.IO packet (no polling-style batching/base64 framing involved).
// Writes are serialized because gorilla/websocket only supports one
// concurrent writer per connection.
type transport struct {
	conn         *websocket.Conn
	writeMu      sync.Mutex
	writeTimeout time.Duration
}

// dial connects to wsURL and consumes the Engine.IO OPEN packet the server
// sends immediately after the websocket handshake.
func dial(ctx context.Context, wsURL string, handshakeTimeout, writeTimeout time.Duration) (*transport, openPacket, error) {
	dialer := websocket.Dialer{HandshakeTimeout: handshakeTimeout}
	conn, resp, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return nil, openPacket{}, fmt.Errorf("socketio2: dial websocket: %w%s", err, handshakeDetail(resp))
	}

	t := &transport{conn: conn, writeTimeout: writeTimeout}

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

// handshakeDetail renders the HTTP response a failed handshake got back.
// gorilla's ErrBadHandshake alone can't distinguish a 403 (source IP not
// whitelisted) from a 400 (bad params) or a proxy that ate the Upgrade, so
// the status line and a bounded body excerpt go into the error.
func handshakeDetail(resp *http.Response) string {
	if resp == nil {
		return ""
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	return fmt.Sprintf(" (HTTP %s: %s)", resp.Status, strings.TrimSpace(string(body)))
}

func (t *transport) readPacket() (eioType, string, error) {
	_, data, err := t.conn.ReadMessage()
	if err != nil {
		return 0, "", err
	}
	return decodeEIOPacket(string(data))
}

// writePacket serializes writes because gorilla/websocket allows only one
// concurrent writer. The write deadline matters as much as the mutex: without
// it a peer that stops reading would block WriteMessage forever while holding
// writeMu, wedging every subsequent Emit on the connection.
func (t *transport) writePacket(typ eioType, payload string) error {
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	if t.writeTimeout > 0 {
		if err := t.conn.SetWriteDeadline(time.Now().Add(t.writeTimeout)); err != nil {
			return err
		}
	}
	return t.conn.WriteMessage(websocket.TextMessage, []byte(encodeEIOPacket(typ, payload)))
}

func (t *transport) setReadDeadline(deadline time.Time) error {
	return t.conn.SetReadDeadline(deadline)
}

func (t *transport) close() error {
	return t.conn.Close()
}
