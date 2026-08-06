package socketio2

import "errors"

// ErrNotConnected is returned by Emit/EmitWithAck when called while the
// client has no active connection (before the first successful connect, or
// while reconnecting).
var ErrNotConnected = errors.New("socketio2: not connected")

// ErrClosed is returned by EmitWithAck when the connection drops before the
// server acknowledges the emitted event.
var ErrClosed = errors.New("socketio2: connection closed before ack was received")
