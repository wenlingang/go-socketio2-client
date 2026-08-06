# go-socketio2-client

A minimal Go client for the **legacy Socket.IO v2 wire protocol** — Engine.IO
v3 handshake + Socket.IO v2 packet framing, i.e. whatever is on the other end
of `"socket.io-client": "2.x"` / `"socket.io": "2.x"` on the Node.js side.

Only the websocket transport is implemented (no HTTP long-polling), which is
sufficient for any server reachable with:

```js
io.connect(host, { transports: ['websocket'] })
```

## Why this exists

There is no maintained Go library for this protocol generation:

- [`googollee/go-socket.io`](https://github.com/googollee/go-socket.io) is
  archived and never supported the Socket.IO v2 protocol.
- The dedicated v2 client forks
  ([`zhouhui8915/go-socket.io-client`](https://github.com/zhouhui8915/go-socket.io-client),
  [`graarh/golang-socketio`](https://github.com/graarh/golang-socketio)) have
  been unmaintained for 5+ years with open protocol-compatibility issues.
- Newer libraries (e.g. `maldikhan/go.socket.io`) target Socket.IO v4/v5,
  which is a different, incompatible wire protocol.

This package was written from — and verified against — the actual packages
bundled with `socket.io-client@2.0.4` (`socket.io-parser@3.1.3`,
`engine.io-parser@2.1.3`), not just the general protocol spec.

## Install

```
go get github.com/wenlingang/go-socketio2-client
```

## Usage

```go
client := socketio2.New(socketio2.Options{
    // The URL path is the Socket.IO namespace, matching how
    // socket.io-client v2's io.connect(host, opts) treats it.
    Host: "https://example.com/my-namespace",
    Query: map[string]string{
        "token": "secret",
    },
})

client.OnConnect(func() {
    // Re-join on every (re)connect — a fresh connection resets any
    // server-side room membership.
    client.Emit("join", map[string]string{"room": "lobby"})
})

client.OnEvent(func(event string, args []json.RawMessage) {
    log.Printf("received %s: %s", event, args[0])
})

client.OnDisconnect(func(err error) {
    log.Printf("disconnected: %v", err)
})

ctx, cancel := context.WithCancel(context.Background())
defer cancel()

// Run blocks, reconnecting forever (matching socket.io-client v2's default
// reconnection: true, reconnectionAttempts: Infinity) until ctx is canceled.
if err := client.Run(ctx); err != nil {
    log.Fatal(err)
}
```

### Waiting for an ack

```go
resp, err := client.EmitWithAck(ctx, "join", map[string]string{"room": "lobby"})
```

## What it does and doesn't do

- Implements the Engine.IO v3 packet types (`OPEN`/`CLOSE`/`PING`/`PONG`/`MESSAGE`/`UPGRADE`/`NOOP`)
  and Socket.IO v2 packet types (`CONNECT`/`DISCONNECT`/`EVENT`/`ACK`/`ERROR`),
  encoded/decoded exactly as `socket.io-parser`/`engine.io-parser` v2-era do.
- Handles server-initiated ping/pong and read-deadline based liveness, since
  in Engine.IO v3 the **server** pings and the client must respond.
- Reconnects automatically with the same defaults as socket.io-client v2
  (1s initial delay, 5s max, 0.5 randomization factor, unlimited attempts by
  default), re-running the CONNECT handshake on every attempt. Your
  `OnConnect` callback is the place to redo any subscribe/join emits.
- Does **not** implement HTTP long-polling, binary/ArrayBuffer packets, or
  multiplexed namespaces on a single connection — one `Client` is one
  namespace, one websocket connection.

## License

MIT
