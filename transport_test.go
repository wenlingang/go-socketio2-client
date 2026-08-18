package socketio2

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestDial_BadHandshakeIncludesStatusAndBody pins down the diagnosability of
// a rejected handshake: gorilla/websocket's bare "bad handshake" hides
// whether the server said 403 (IP not whitelisted), 400 (bad params) or
// something else, which makes production triage guesswork. The dial error
// must surface the HTTP status and response body the server actually sent.
func TestDial_BadHandshakeIncludesStatusAndBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"code":"E4003","message":"The URL resource forbidden"}`))
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/socket.io/?EIO=3&transport=websocket"

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _, err := dial(ctx, wsURL, time.Second, time.Second)
	if err == nil {
		t.Fatal("expected dial to fail against a 403 response")
	}
	for _, want := range []string{"403", "E4003"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("dial error should contain %q, got: %v", want, err)
		}
	}
}
