// WebSocket client: connection lifecycle and keepalive.
//
// This file provides the WebSocketClient used when ClientType is WebsocketConnClient.
// Send/Receive/Listen live in websocket_io.go. Connection is always established by
// Dial; SkipConnectivityCheck is ignored for WebSocket.
package network

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// WebSocketClient is a WebSocket connection client. Create via
// NewConnection(WebsocketConnClient) and AsWebSocketConnectionType. All methods are
// safe for concurrent use.
type WebSocketClient struct {
	conn              *websocket.Conn
	ConnectionOptions // URL, Timeout, Headers (used on handshake)
	dialer            *websocket.Dialer
	pathIndex         int
	mu                sync.RWMutex
	ctx               context.Context
	cancel            context.CancelFunc
	autoReconnect     bool
	reconnectDelay    time.Duration
}

// convertToHTTPHeader builds an http.Header from a string map for the handshake.
func convertToHTTPHeader(headers map[string]string) http.Header {
	httpHeaders := http.Header{}
	for key, value := range headers {
		httpHeaders.Add(key, value)
	}
	return httpHeaders
}

// Connect establishes the WebSocket connection to the URL derived from opts and
// pathIndex. If opts.Timeout <= 0, DefaultTimeout is used for the handshake. On
// success a ping/pong keepalive goroutine is started. Returns an error if the
// handshake fails.
func (ws *WebSocketClient) Connect(opts ConnectionOptions) error {
	if opts.Timeout <= 0 {
		opts.Timeout = DefaultTimeout
	}
	ws.mu.Lock()
	defer ws.mu.Unlock()

	ws.ConnectionOptions = opts
	ws.dialer = &websocket.Dialer{HandshakeTimeout: opts.Timeout}
	if ws.reconnectDelay == 0 {
		ws.reconnectDelay = 5 * time.Second
	}
	ws.ctx, ws.cancel = context.WithCancel(context.Background())

	fullURL, err := buildFullURL(opts.URL, ws.pathIndex)
	if err != nil {
		return fmt.Errorf("failed to build WebSocket URL: %w", err)
	}

	conn, resp, err := ws.dialer.Dial(fullURL, convertToHTTPHeader(opts.Headers))
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	if err != nil {
		return fmt.Errorf("failed to dial WebSocket at %s: %w", opts.URL.Host, err)
	}
	ws.conn = conn
	ws.startPingPong()
	return nil
}

// Close sends a close frame, closes the connection, cancels the connection context
// (stopping the ping goroutine), and clears the client state. Safe to call repeatedly.
func (ws *WebSocketClient) Close() error {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	if ws.cancel != nil {
		ws.cancel()
	}
	if ws.conn != nil {
		_ = ws.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		err := ws.conn.Close()
		ws.conn = nil
		return err
	}
	return nil
}

// Reconnect closes the current connection (if any) and calls Connect with the same
// ConnectionOptions. The current state is read under the lock before Close/Connect
// (which take the lock themselves), avoiding a data race with a concurrent Close.
func (ws *WebSocketClient) Reconnect() error {
	ws.mu.RLock()
	open := ws.conn != nil
	opts := ws.ConnectionOptions
	ws.mu.RUnlock()
	if open {
		_ = ws.Close()
	}
	return ws.Connect(opts)
}

// SetAutoReconnect enables or disables automatic reconnection in Listen. When enabled,
// a read error in Listen triggers a sleep for delay (or the default 5s if delay <= 0),
// then Reconnect; on success, listening continues.
func (ws *WebSocketClient) SetAutoReconnect(enabled bool, delay time.Duration) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	ws.autoReconnect = enabled
	if delay > 0 {
		ws.reconnectDelay = delay
	}
}

// startPingPong sends a ping every 30 seconds and refreshes the read deadline on pong.
// The goroutine exits when ws.ctx is cancelled (e.g. on Close).
func (ws *WebSocketClient) startPingPong() {
	ws.conn.SetPongHandler(func(string) error {
		return ws.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	})
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				ws.mu.RLock()
				conn := ws.conn
				ws.mu.RUnlock()
				if conn != nil {
					if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
						return
					}
				}
			case <-ws.ctx.Done():
				return
			}
		}
	}()
}
