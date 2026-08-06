package socketio2

import (
	"encoding/json"
	"testing"
)

func TestEncodeDecodeEIOPacket(t *testing.T) {
	frame := encodeEIOPacket(eioMessage, "40/chat,")
	if frame != "440/chat," {
		t.Fatalf("unexpected frame: %q", frame)
	}

	typ, payload, err := decodeEIOPacket(frame)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if typ != eioMessage {
		t.Fatalf("unexpected type: %q", byte(typ))
	}
	if payload != "40/chat," {
		t.Fatalf("unexpected payload: %q", payload)
	}
}

func TestEncodeSIOPacket_Connect(t *testing.T) {
	got := encodeSIOPacket(sioPacket{Type: sioConnect, Nsp: "/chat"})
	want := "0/chat,"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestEncodeSIOPacket_EventWithAck(t *testing.T) {
	data, err := encodeEventData("join", map[string]string{"room": "lobby"})
	if err != nil {
		t.Fatalf("encodeEventData error: %v", err)
	}

	got := encodeSIOPacket(sioPacket{Type: sioEvent, Nsp: "/chat", HasAck: true, AckID: 0, Data: data})
	want := `2/chat,0["join",{"room":"lobby"}]`
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestDecodeSIOPacket_ConnectWithSid(t *testing.T) {
	p, err := decodeSIOPacket(`0/chat,{"sid":"abc123"}`)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if p.Type != sioConnect || p.Nsp != "/chat" || p.HasAck {
		t.Fatalf("unexpected packet: %+v", p)
	}
	var sid struct {
		SID string `json:"sid"`
	}
	if err := json.Unmarshal(p.Data, &sid); err != nil {
		t.Fatalf("unmarshal data error: %v", err)
	}
	if sid.SID != "abc123" {
		t.Fatalf("unexpected sid: %q", sid.SID)
	}
}

func TestDecodeSIOPacket_EventNoAck(t *testing.T) {
	p, err := decodeSIOPacket(`2/chat,["message",{"kind":"greeting","body":"hi there"}]`)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if p.Type != sioEvent || p.Nsp != "/chat" || p.HasAck {
		t.Fatalf("unexpected packet: %+v", p)
	}

	event, args, err := DecodeEventData(p.Data)
	if err != nil {
		t.Fatalf("DecodeEventData error: %v", err)
	}
	if event != "message" {
		t.Fatalf("unexpected event: %q", event)
	}
	if len(args) != 1 {
		t.Fatalf("unexpected args: %+v", args)
	}
	var payload map[string]any
	if err := json.Unmarshal(args[0], &payload); err != nil {
		t.Fatalf("unmarshal payload error: %v", err)
	}
	if payload["kind"] != "greeting" {
		t.Fatalf("unexpected kind: %v", payload["kind"])
	}
}

func TestDecodeSIOPacket_AckWithID(t *testing.T) {
	p, err := decodeSIOPacket(`3/chat,0[{"status":"ok"}]`)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if p.Type != sioAck || p.Nsp != "/chat" || !p.HasAck || p.AckID != 0 {
		t.Fatalf("unexpected packet: %+v", p)
	}
	if string(p.Data) != `[{"status":"ok"}]` {
		t.Fatalf("unexpected data: %s", p.Data)
	}
}

func TestDecodeSIOPacket_DefaultNamespace(t *testing.T) {
	p, err := decodeSIOPacket(`0`)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if p.Type != sioConnect || p.Nsp != "/" {
		t.Fatalf("unexpected packet: %+v", p)
	}
}

func TestBuildWebsocketURL(t *testing.T) {
	wsURL, nsp, err := buildWebsocketURL(Options{
		Host:  "https://example.com/chat",
		Query: map[string]string{"apiKey": "demo-key", "token": "secret"},
	})
	if err != nil {
		t.Fatalf("buildWebsocketURL error: %v", err)
	}
	if nsp != "/chat" {
		t.Fatalf("unexpected nsp: %q", nsp)
	}
	want := "wss://example.com/socket.io/?EIO=3&apiKey=demo-key&token=secret&transport=websocket"
	if wsURL != want {
		t.Fatalf("got %q, want %q", wsURL, want)
	}
}

func TestBuildWebsocketURL_CustomPath(t *testing.T) {
	wsURL, nsp, err := buildWebsocketURL(Options{
		Host: "http://localhost:9011/ns",
		Path: "/custom.io/",
	})
	if err != nil {
		t.Fatalf("buildWebsocketURL error: %v", err)
	}
	if nsp != "/ns" {
		t.Fatalf("unexpected nsp: %q", nsp)
	}
	want := "ws://localhost:9011/custom.io/?EIO=3&transport=websocket"
	if wsURL != want {
		t.Fatalf("got %q, want %q", wsURL, want)
	}
}
