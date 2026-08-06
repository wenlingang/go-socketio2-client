package socketio2

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// Engine.IO v3 packet types (github.com/socketio/engine.io-parser@2.x, protocol 3).
type eioType byte

const (
	eioOpen    eioType = '0'
	eioClose   eioType = '1'
	eioPing    eioType = '2'
	eioPong    eioType = '3'
	eioMessage eioType = '4'
	eioUpgrade eioType = '5'
	eioNoop    eioType = '6'
)

// Socket.IO v2 packet types (github.com/socketio/socket.io-parser@3.x, protocol 4).
type sioType int

const (
	sioConnect    sioType = 0
	sioDisconnect sioType = 1
	sioEvent      sioType = 2
	sioAck        sioType = 3
	sioError      sioType = 4
)

// openPacket is the JSON payload of the Engine.IO OPEN packet the server
// sends immediately after the handshake.
type openPacket struct {
	SID          string   `json:"sid"`
	Upgrades     []string `json:"upgrades"`
	PingInterval int      `json:"pingInterval"`
	PingTimeout  int      `json:"pingTimeout"`
}

type sioPacket struct {
	Type   sioType
	Nsp    string
	HasAck bool
	AckID  int
	Data   json.RawMessage
}

func encodeEIOPacket(t eioType, payload string) string {
	return string(byte(t)) + payload
}

func decodeEIOPacket(frame string) (eioType, string, error) {
	if len(frame) == 0 {
		return 0, "", fmt.Errorf("socketio2: empty engine.io frame")
	}
	return eioType(frame[0]), frame[1:], nil
}

// encodeSIOPacket mirrors socket.io-parser v2's encodeAsString: type digit,
// then "<nsp>," when nsp isn't the default namespace, then an optional ack
// id, then the JSON-encoded data. Verified against the real socket.io-parser
// (v3.1.3, bundled with socket.io-client 2.0.4) source.
func encodeSIOPacket(p sioPacket) string {
	var b strings.Builder
	b.WriteString(strconv.Itoa(int(p.Type)))
	if p.Nsp != "" && p.Nsp != "/" {
		b.WriteString(p.Nsp)
		b.WriteByte(',')
	}
	if p.HasAck {
		b.WriteString(strconv.Itoa(p.AckID))
	}
	if len(p.Data) > 0 {
		b.Write(p.Data)
	}
	return b.String()
}

func decodeSIOPacket(s string) (sioPacket, error) {
	if len(s) == 0 {
		return sioPacket{}, fmt.Errorf("socketio2: empty socket.io packet")
	}

	typ, err := strconv.Atoi(s[:1])
	if err != nil {
		return sioPacket{}, fmt.Errorf("socketio2: invalid socket.io packet type %q: %w", s[:1], err)
	}

	rest := s[1:]
	p := sioPacket{Type: sioType(typ), Nsp: "/"}

	if strings.HasPrefix(rest, "/") {
		if idx := strings.IndexByte(rest, ','); idx != -1 {
			p.Nsp = rest[:idx]
			rest = rest[idx+1:]
		} else {
			p.Nsp = rest
			rest = ""
		}
	}

	i := 0
	for i < len(rest) && rest[i] >= '0' && rest[i] <= '9' {
		i++
	}
	if i > 0 {
		ackID, _ := strconv.Atoi(rest[:i])
		p.HasAck = true
		p.AckID = ackID
		rest = rest[i:]
	}

	if len(rest) > 0 {
		p.Data = json.RawMessage(rest)
	}

	return p, nil
}

// DecodeEventData decodes a socket.io EVENT packet's data array
// (`["eventName", arg1, arg2, ...]`) into the event name and remaining args.
func DecodeEventData(data json.RawMessage) (event string, args []json.RawMessage, err error) {
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return "", nil, fmt.Errorf("socketio2: decode event array: %w", err)
	}
	if len(raw) == 0 {
		return "", nil, fmt.Errorf("socketio2: empty event data")
	}
	if err := json.Unmarshal(raw[0], &event); err != nil {
		return "", nil, fmt.Errorf("socketio2: decode event name: %w", err)
	}
	return event, raw[1:], nil
}

// encodeEventData encodes an EVENT packet's data array for `emit(event, payload)`.
func encodeEventData(event string, payload any) (json.RawMessage, error) {
	args := []any{event, payload}
	data, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(data), nil
}
