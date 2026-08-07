package socketio2

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// TestClient_EmitBeforeConnectIsRefused guards the emit gate: a Socket.IO v2
// server silently drops packets for a namespace it has not yet connected, so
// emitting early must fail loudly rather than lose the event.
func TestClient_EmitBeforeConnectIsRefused(t *testing.T) {
	client := New(Options{Host: "https://example.com/chat"})

	if err := client.Emit("join", map[string]string{"room": "lobby"}); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("expected ErrNotConnected, got %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := client.EmitWithAck(ctx, "join", map[string]string{"room": "lobby"}); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("expected ErrNotConnected from EmitWithAck, got %v", err)
	}
}

// TestClient_SendsClientInitiatedHeartbeat pins down the Engine.IO v3
// heartbeat direction: the CLIENT must send PING every pingInterval and the
// server answers PONG. (Engine.IO v4 inverts this.) A client that waits for
// a server ping instead would be dropped as dead by a v3 server.
func TestClient_SendsClientInitiatedHeartbeat(t *testing.T) {
	pings := make(chan struct{}, 8)

	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		// Deliberately short pingInterval so the test stays fast.
		if err := conn.WriteMessage(websocket.TextMessage,
			[]byte(`0{"sid":"test-sid","pingInterval":50,"pingTimeout":2000}`)); err != nil {
			return
		}

		for {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if string(raw) != "2" {
				continue
			}
			select {
			case pings <- struct{}{}:
			default:
			}
			if err := conn.WriteMessage(websocket.TextMessage, []byte("3")); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	client := New(Options{Host: server.URL})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		_, _ = client.connectAndServe(ctx)
		close(done)
	}()

	for i := 0; i < 3; i++ {
		select {
		case <-pings:
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for client ping #%d — the client is not driving the heartbeat", i+1)
		}
	}

	cancel()
	<-done
}

// TestClient_DefaultNamespaceSendsNoConnectPacket mirrors
// socket.io-client v2's `if ('/' !== this.nsp)` guard: on the default
// namespace the server connects the socket itself, and a client-sent
// CONNECT would register a duplicate socket.
func TestClient_DefaultNamespaceSendsNoConnectPacket(t *testing.T) {
	unexpected := make(chan string, 4)

	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		// Long pingInterval so no heartbeat traffic muddies the assertion.
		if err := conn.WriteMessage(websocket.TextMessage,
			[]byte(`0{"sid":"test-sid","pingInterval":30000,"pingTimeout":30000}`)); err != nil {
			return
		}
		// The server is the one that connects the default namespace.
		if err := conn.WriteMessage(websocket.TextMessage, []byte("40")); err != nil {
			return
		}

		_ = conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
		for {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				return // expected: read deadline expires with nothing sent
			}
			select {
			case unexpected <- string(raw):
			default:
			}
		}
	}))
	defer server.Close()

	client := New(Options{Host: server.URL})
	connected := make(chan struct{})
	var once sync.Once
	client.OnConnect(func() { once.Do(func() { close(connected) }) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		_, _ = client.connectAndServe(ctx)
		close(done)
	}()

	select {
	case <-connected:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for server-initiated CONNECT to reach OnConnect")
	}

	select {
	case frame := <-unexpected:
		t.Fatalf("client sent %q on the default namespace; socket.io-client v2 sends nothing", frame)
	case <-time.After(400 * time.Millisecond):
	}

	cancel()
	<-done
}

// TestClient_ConnectEmitAndReceive runs a fake Socket.IO v2 server over a
// real websocket connection and asserts that Client performs the Engine.IO
// v3/Socket.IO v2 handshake, emits a "join" request from OnConnect, acks it,
// and delivers a subsequent "message" EVENT packet to OnEvent.
func TestClient_ConnectEmitAndReceive(t *testing.T) {
	var joinedRoom string
	var joinMu sync.Mutex

	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade error: %v", err)
			return
		}
		defer conn.Close()

		open := openPacket{SID: "test-sid", PingInterval: 25000, PingTimeout: 20000}
		openBytes, _ := json.Marshal(open)
		if err := conn.WriteMessage(websocket.TextMessage, []byte(encodeEIOPacket(eioOpen, string(openBytes)))); err != nil {
			t.Errorf("write open error: %v", err)
			return
		}

		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Errorf("read connect error: %v", err)
			return
		}
		_, payload, _ := decodeEIOPacket(string(raw))
		p, _ := decodeSIOPacket(payload)
		// socket.io-client v2 appends the socket-level query to the namespace
		// in the CONNECT packet, so the server sees /chat?apiKey=...&token=...
		nspPath, nspQuery, _ := strings.Cut(p.Nsp, "?")
		if p.Type != sioConnect || nspPath != "/chat" {
			t.Errorf("unexpected connect packet: %+v", p)
			return
		}
		parsedQuery, err := url.ParseQuery(nspQuery)
		if err != nil {
			t.Errorf("parse connect namespace query %q: %v", nspQuery, err)
			return
		}
		if parsedQuery.Get("apiKey") != "demo-key" || parsedQuery.Get("token") != "test-token" {
			t.Errorf("connect packet missing query credentials: %q", p.Nsp)
			return
		}

		connectAck := encodeSIOPacket(sioPacket{Type: sioConnect, Nsp: "/chat"})
		if err := conn.WriteMessage(websocket.TextMessage, []byte(encodeEIOPacket(eioMessage, connectAck))); err != nil {
			t.Errorf("write connect ack error: %v", err)
			return
		}

		_, raw, err = conn.ReadMessage()
		if err != nil {
			t.Errorf("read join error: %v", err)
			return
		}
		_, payload, _ = decodeEIOPacket(string(raw))
		p, _ = decodeSIOPacket(payload)
		event, args, _ := DecodeEventData(p.Data)
		if event == "join" && len(args) > 0 {
			var joinPayload struct {
				Room string `json:"room"`
			}
			_ = json.Unmarshal(args[0], &joinPayload)
			joinMu.Lock()
			joinedRoom = joinPayload.Room
			joinMu.Unlock()
		}

		ackData, _ := json.Marshal([]any{map[string]string{"status": "ok"}})
		ackPacket := encodeSIOPacket(sioPacket{Type: sioAck, Nsp: "/chat", HasAck: true, AckID: p.AckID, Data: ackData})
		if err := conn.WriteMessage(websocket.TextMessage, []byte(encodeEIOPacket(eioMessage, ackPacket))); err != nil {
			t.Errorf("write ack error: %v", err)
			return
		}

		messageData, _ := encodeEventData("message", map[string]any{"kind": "greeting", "body": "hello"})
		messagePacket := encodeSIOPacket(sioPacket{Type: sioEvent, Nsp: "/chat", Data: messageData})
		if err := conn.WriteMessage(websocket.TextMessage, []byte(encodeEIOPacket(eioMessage, messagePacket))); err != nil {
			t.Errorf("write message event error: %v", err)
			return
		}

		time.Sleep(100 * time.Millisecond)
	}))
	defer server.Close()

	client := New(Options{
		Host:  server.URL + "/chat",
		Query: map[string]string{"apiKey": "demo-key", "token": "test-token"},
	})

	received := make(chan struct {
		event string
		args  []json.RawMessage
	}, 1)
	client.OnEvent(func(event string, args []json.RawMessage) {
		received <- struct {
			event string
			args  []json.RawMessage
		}{event, args}
	})
	client.OnConnect(func() {
		if err := client.Emit("join", map[string]string{"room": "lobby"}); err != nil {
			t.Errorf("emit join error: %v", err)
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := client.connectAndServe(ctx)
		done <- err
	}()

	select {
	case got := <-received:
		if got.event != "message" {
			t.Fatalf("unexpected event: %q", got.event)
		}
		var payload map[string]any
		if err := json.Unmarshal(got.args[0], &payload); err != nil {
			t.Fatalf("unmarshal payload error: %v", err)
		}
		if payload["kind"] != "greeting" {
			t.Fatalf("unexpected payload: %v", payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event")
	}

	joinMu.Lock()
	if joinedRoom != "lobby" {
		t.Fatalf("expected server to receive a join emit for room 'lobby', got %q", joinedRoom)
	}
	joinMu.Unlock()

	cancel()
	<-done
}
