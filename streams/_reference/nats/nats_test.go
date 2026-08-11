package nats

import (
	"testing"

	_ "github.com/joho/godotenv/autoload"
	"github.com/nats-io/nats.go"
)

func TestNatsHandler_ConnReturnsUnderlyingClient(t *testing.T) {
	// Arrange: create a dummy *nats.Conn pointer (nil is fine for identity checks)
	var dummy *nats.Conn
	h := &natsHandler{client: dummy}

	// Act
	got := h.Conn()

	// Assert
	if got != dummy {
		t.Fatalf("expected %p, got %p", dummy, got)
	}
}

func TestNatsHandler_ImplementsNatsManager(t *testing.T) {
	var _ natsManager = (*natsHandler)(nil)
}
