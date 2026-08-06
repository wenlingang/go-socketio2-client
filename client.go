// Package socketio2 is a minimal client for the legacy Socket.IO v2 wire
// protocol (Engine.IO v3 handshake + Socket.IO v2 packet framing, the pair
// used by "socket.io-client": "2.x" and "socket.io": "2.x" on the server
// side). It only implements the websocket transport (no HTTP long-polling),
// which is sufficient for any server reachable with
// `io.connect(host, { transports: ['websocket'] })` on the Node.js side.
//
// There is no maintained Go library for this protocol generation:
// googollee/go-socket.io is archived and never supported Socket.IO v2, and
// the handful of dedicated v2 client forks (zhouhui8915/go-socket.io-client,
// graarh/golang-socketio) have been unmaintained for 5+ years. This package
// was written from, and verified against, the actual packages bundled with
// socket.io-client@2.0.4 (socket.io-parser@3.1.3, engine.io-parser@2.1.3).
package socketio2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// EventHandler is called for every Socket.IO EVENT packet received on the
// namespace, with the event name and its remaining arguments.
type EventHandler func(event string, args []json.RawMessage)

// Client is a reconnecting Socket.IO v2 client for a single namespace.
// A Client is safe for concurrent use: Emit/EmitWithAck may be called from
// any goroutine while Run is driving the connection.
type Client struct {
	opts Options

	mu        sync.Mutex
	transport *transport
	nsp       string

	ackMu      sync.Mutex
	ackCounter int
	ackWaiters map[int]chan ackResult

	onConnect    func()
	onDisconnect func(error)
	onEvent      EventHandler
	onError      func(error)
}

type ackResult struct {
	data json.RawMessage
	err  error
}

// New creates a Client. It does not connect until Run is called.
func New(opts Options) *Client {
	return &Client{
		opts:       opts,
		ackWaiters: make(map[int]chan ackResult),
	}
}

// OnConnect registers a callback invoked every time the Socket.IO namespace
// CONNECT handshake completes, including after a reconnect. This is the
// place to (re-)emit subscription requests, since a fresh connection
// resets any server-side subscription state.
func (c *Client) OnConnect(handler func()) { c.onConnect = handler }

// OnDisconnect registers a callback invoked whenever the connection drops,
// with the error that caused it (nil if it was a clean server disconnect).
func (c *Client) OnDisconnect(handler func(err error)) { c.onDisconnect = handler }

// OnEvent registers the callback invoked for every received EVENT packet.
func (c *Client) OnEvent(handler EventHandler) { c.onEvent = handler }

// OnError registers a callback invoked for protocol-level errors that don't
// terminate the connection (e.g. a malformed packet, a Socket.IO ERROR
// packet). Fatal connection errors are instead reported via OnDisconnect.
func (c *Client) OnError(handler func(error)) { c.onError = handler }

// Run drives the connect/reconnect loop until ctx is canceled or
// MaxReconnectAttempts consecutive failures occur, whichever happens first.
// It blocks; run it in its own goroutine.
func (c *Client) Run(ctx context.Context) error {
	delay := c.opts.reconnectionDelay()
	attempts := 0

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		connected, err := c.connectAndServe(ctx)
		if c.onDisconnect != nil {
			c.onDisconnect(err)
		}

		if connected {
			delay = c.opts.reconnectionDelay()
			attempts = 0
		} else {
			attempts++
			if c.opts.MaxReconnectAttempts > 0 && attempts >= c.opts.MaxReconnectAttempts {
				return fmt.Errorf("socketio2: giving up after %d reconnect attempts: %w", attempts, err)
			}
			delay = nextDelay(delay, c.opts.reconnectionDelayMax())
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		select {
		case <-time.After(jitter(delay, c.opts.randomizationFactor())):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Close closes the current underlying connection, if any, which causes Run
// to observe a read error and proceed to its normal reconnect flow. To stop
// Run for good, cancel the context passed to it instead.
func (c *Client) Close() error {
	c.mu.Lock()
	t := c.transport
	c.mu.Unlock()
	if t == nil {
		return nil
	}
	return t.close()
}

// Emit sends an EVENT packet without requesting an acknowledgement.
func (c *Client) Emit(event string, payload any) error {
	return c.emit(event, payload, false, 0)
}

// EmitWithAck sends an EVENT packet with an ack id and blocks until the
// server acknowledges it, ctx is done, or the connection drops.
func (c *Client) EmitWithAck(ctx context.Context, event string, payload any) (json.RawMessage, error) {
	c.ackMu.Lock()
	id := c.ackCounter
	c.ackCounter++
	ch := make(chan ackResult, 1)
	c.ackWaiters[id] = ch
	c.ackMu.Unlock()

	defer func() {
		c.ackMu.Lock()
		delete(c.ackWaiters, id)
		c.ackMu.Unlock()
	}()

	if err := c.emit(event, payload, true, id); err != nil {
		return nil, err
	}

	select {
	case res := <-ch:
		return res.data, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *Client) emit(event string, payload any, hasAck bool, ackID int) error {
	c.mu.Lock()
	t := c.transport
	nsp := c.nsp
	c.mu.Unlock()

	if t == nil {
		return ErrNotConnected
	}

	data, err := encodeEventData(event, payload)
	if err != nil {
		return err
	}

	packet := sioPacket{Type: sioEvent, Nsp: nsp, HasAck: hasAck, AckID: ackID, Data: data}
	return t.writePacket(eioMessage, encodeSIOPacket(packet))
}

func (c *Client) resolveAck(id int, data json.RawMessage) {
	c.ackMu.Lock()
	ch, ok := c.ackWaiters[id]
	c.ackMu.Unlock()
	if ok {
		ch <- ackResult{data: data}
	}
}

func (c *Client) failPendingAcks() {
	c.ackMu.Lock()
	waiters := c.ackWaiters
	c.ackWaiters = make(map[int]chan ackResult)
	c.ackMu.Unlock()
	for _, ch := range waiters {
		ch <- ackResult{err: ErrClosed}
	}
}

// connectAndServe dials the server, performs the Socket.IO CONNECT
// handshake, and then blocks reading packets until the connection drops or
// ctx is canceled. The returned bool reports whether the namespace CONNECT
// ever succeeded, so Run can reset its reconnect backoff.
func (c *Client) connectAndServe(ctx context.Context) (connected bool, err error) {
	wsURL, nsp, err := buildWebsocketURL(c.opts)
	if err != nil {
		return false, err
	}

	t, open, err := dial(ctx, wsURL, c.opts.dialTimeout())
	if err != nil {
		return false, err
	}
	defer t.close()
	defer c.failPendingAcks()

	c.mu.Lock()
	c.transport = t
	c.nsp = nsp
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.transport = nil
		c.mu.Unlock()
	}()

	if err := t.writePacket(eioMessage, encodeSIOPacket(sioPacket{Type: sioConnect, Nsp: nsp})); err != nil {
		return false, fmt.Errorf("socketio2: send connect packet: %w", err)
	}

	pingTimeout := time.Duration(open.PingTimeout) * time.Millisecond
	if pingTimeout <= 0 {
		pingTimeout = 60 * time.Second
	}
	_ = t.setReadDeadline(time.Now().Add(pingTimeout))

	for {
		if ctx.Err() != nil {
			return connected, ctx.Err()
		}

		eType, payload, err := t.readPacket()
		if err != nil {
			return connected, err
		}

		switch eType {
		case eioPing:
			if err := t.writePacket(eioPong, payload); err != nil {
				return connected, fmt.Errorf("socketio2: send pong: %w", err)
			}
			_ = t.setReadDeadline(time.Now().Add(pingTimeout))
		case eioClose:
			return connected, errors.New("socketio2: server sent close packet")
		case eioMessage:
			sp, err := decodeSIOPacket(payload)
			if err != nil {
				if c.onError != nil {
					c.onError(err)
				}
				continue
			}
			if sp.Nsp != nsp {
				continue
			}

			switch sp.Type {
			case sioConnect:
				if !connected {
					connected = true
					if c.onConnect != nil {
						c.onConnect()
					}
				}
			case sioEvent:
				if c.onEvent == nil {
					continue
				}
				event, args, err := DecodeEventData(sp.Data)
				if err != nil {
					if c.onError != nil {
						c.onError(err)
					}
					continue
				}
				c.onEvent(event, args)
			case sioAck:
				c.resolveAck(sp.AckID, sp.Data)
			case sioError:
				if c.onError != nil {
					c.onError(fmt.Errorf("socketio2: socket.io error packet: %s", sp.Data))
				}
			case sioDisconnect:
				return connected, errors.New("socketio2: server sent disconnect packet")
			}
		}
	}
}

// nextDelay doubles the delay (mirroring backo2's default factor of 2, the
// library socket.io-client v2 uses for reconnection), capped at max.
func nextDelay(d, max time.Duration) time.Duration {
	next := d * 2
	if next > max {
		return max
	}
	return next
}

// jitter applies a symmetric +/- randomizationFactor deviation around d,
// approximating socket.io-client v2's default reconnection jitter.
func jitter(d time.Duration, randomizationFactor float64) time.Duration {
	delta := float64(d) * randomizationFactor
	base := float64(d) - delta
	return time.Duration(base + rand.Float64()*2*delta)
}
