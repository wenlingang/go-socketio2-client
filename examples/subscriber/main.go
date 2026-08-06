// Command subscriber is a minimal example of using socketio2 to join a room
// on a Socket.IO v2 server and log every chat message it receives.
package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"

	socketio2 "github.com/wenlingang/go-socketio2-client"
)

func main() {
	client := socketio2.New(socketio2.Options{
		Host: "https://example.com/chat",
		Query: map[string]string{
			"apiKey": os.Getenv("API_KEY"),
			"token":  os.Getenv("TOKEN"),
		},
	})

	client.OnConnect(func() {
		if err := client.Emit("join", map[string]string{"room": "lobby"}); err != nil {
			log.Printf("emit join failed: %v", err)
		}
	})

	client.OnEvent(func(event string, args []json.RawMessage) {
		if event != "message" || len(args) == 0 {
			return
		}
		var payload struct {
			Body string `json:"body"`
		}
		if err := json.Unmarshal(args[0], &payload); err != nil {
			return
		}
		log.Printf("message: %s", payload.Body)
	})

	client.OnDisconnect(func(err error) {
		log.Printf("disconnected: %v", err)
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if err := client.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}
