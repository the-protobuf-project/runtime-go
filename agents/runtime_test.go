package agents

import (
	"context"
	"errors"
	"testing"
)

func TestNew_Defaults(t *testing.T) {
	cfg := New(Config{Name: "svc"}).Config()
	if cfg.Host != DefaultHost {
		t.Errorf("Host = %q, want %q", cfg.Host, DefaultHost)
	}
	if cfg.ReadyTimeout != DefaultReadyTimeout {
		t.Errorf("ReadyTimeout = %v, want %v", cfg.ReadyTimeout, DefaultReadyTimeout)
	}
}

func TestStart_NoServices(t *testing.T) {
	if err := New(Config{Name: "svc"}).Start(t.Context()); !errors.Is(err, ErrNoServices) {
		t.Fatalf("got %v, want ErrNoServices", err)
	}
}

func TestStart_Twice(t *testing.T) {
	rt := New(Config{Name: "svc", Host: "127.0.0.1", Port: freePort(t)})
	rt.Register(newFake(MCP, Requirements{HTTP: true}, "/mcp"))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if err := rt.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = rt.Shutdown(t.Context()) }()

	if err := rt.Start(ctx); !errors.Is(err, ErrAlreadyStarted) {
		t.Fatalf("got %v, want ErrAlreadyStarted", err)
	}
}

// Registering after start could only be dropped on the floor, so it is a
// programming error rather than a silent no-op.
func TestRegister_AfterStartPanics(t *testing.T) {
	rt := New(Config{Name: "svc", Host: "127.0.0.1", Port: freePort(t)})
	rt.Register(newFake(MCP, Requirements{HTTP: true}, "/mcp"))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if err := rt.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = rt.Shutdown(t.Context()) }()

	defer func() {
		if recover() == nil {
			t.Error("expected a panic from Register after Start")
		}
	}()
	rt.Register(newFake(A2A, Requirements{HTTP: true}, "/a2a"))
}

// The whole point of one object: two protocols, one port, each under its own
// path.
