// WebSocket I/O: Send, Receive, RetrySend, and Listen.
package network

import (
	"context"
	"fmt"
	"time"
)

// Send writes a single WebSocket frame. messageType is typically websocket.TextMessage
// or websocket.BinaryMessage. Returns an error if the connection is closed or the
// write fails.
func (ws *WebSocketClient) Send(messageType int, message []byte) error {
	ws.mu.RLock()
	defer ws.mu.RUnlock()
	if ws.conn == nil {
		return fmt.Errorf("no WebSocket connection available")
	}
	if err := ws.conn.WriteMessage(messageType, message); err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}
	return nil
}

// Receive reads the next WebSocket message. It blocks until a message is available or
// the connection is closed. Returns the frame type, payload, and an error on failure.
func (ws *WebSocketClient) Receive() (messageType int, message []byte, err error) {
	ws.mu.RLock()
	conn := ws.conn
	ws.mu.RUnlock()
	if conn == nil {
		return 0, nil, fmt.Errorf("no WebSocket connection available")
	}
	messageType, message, err = conn.ReadMessage()
	if err != nil {
		return 0, nil, fmt.Errorf("failed to read message: %w", err)
	}
	return messageType, message, nil
}

// RetrySend sends a message with up to maxRetries attempts, sleeping 2 seconds between
// attempts. Returns nil on first success, or an error after all retries fail.
func (ws *WebSocketClient) RetrySend(messageType int, message []byte, maxRetries int) error {
	var err error
	for i := 0; i < maxRetries; i++ {
		if err = ws.Send(messageType, message); err == nil {
			return nil
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("failed to send message after %d retries: %w", maxRetries, err)
}

// Listen reads messages in a loop and calls handleMessage for each. It returns a
// channel that receives a single error when the loop stops (context cancelled,
// connection closed, or read error; if auto-reconnect is enabled and reconnection
// fails, that error is sent). The channel is closed after the error is sent.
func (ws *WebSocketClient) Listen(ctx context.Context, handleMessage func(messageType int, message []byte)) <-chan error {
	errChan := make(chan error, 1)
	go func() {
		defer close(errChan)
		for {
			select {
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			case <-ws.ctx.Done():
				errChan <- ws.ctx.Err()
				return
			default:
				messageType, message, err := ws.Receive()
				if err != nil {
					if ws.autoReconnect {
						time.Sleep(ws.reconnectDelay)
						if reconnectErr := ws.Reconnect(); reconnectErr != nil {
							errChan <- fmt.Errorf("reconnection failed: %w", reconnectErr)
							return
						}
						continue
					}
					errChan <- err
					return
				}
				handleMessage(messageType, message)
			}
		}
	}()
	return errChan
}
