package socketio2

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

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
		if p.Type != sioConnect || p.Nsp != "/chat" {
			t.Errorf("unexpected connect packet: %+v", p)
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
