package network

// import (
// 	"errors"
// 	"net/http"
// 	"testing"
// 	"time"

// 	"github.com/gorilla/websocket"
// 	"github.com/stretchr/testify/assert"
// )

// // MockWebSocketConn simulates a WebSocket connection for testing purposes.
// type MockWebSocketConn struct {
// 	WrittenMessageType int
// 	WrittenMessage     []byte
// 	ReadMessageType    int
// 	ReadMessage        []byte
// 	ReadError          error
// 	CloseError         error
// }

// func (m *MockWebSocketConn) WriteMessage(messageType int, data []byte) error {
// 	m.WrittenMessageType = messageType
// 	m.WrittenMessage = data
// 	return nil
// }

// func (m *MockWebSocketConn) ReadMessage() (messageType int, p []byte, err error) {
// 	return m.ReadMessageType, m.ReadMessage, m.ReadError
// }

// func (m *MockWebSocketConn) Close() error {
// 	return m.CloseError
// }

// // MockWebSocketDialer is a mock implementation of the websocket.Dialer.
// type MockWebSocketDialer struct {
// 	MockConn *MockWebSocketConn
// 	MockErr  error
// }

// func (m *MockWebSocketDialer) Dial(url string, requestHeader map[string][]string) (*websocket.Conn, *http.Response, error) {
// 	if m.MockErr != nil {
// 		return nil, m.MockErr //nolint:dupword // Commented code
// 	}
// 	// Here we simulate returning the mocked WebSocket connection.
// 	return (*websocket.Conn)(m.MockConn), nil, nil
// }

// func TestWebSocketClientConnectSuccess(t *testing.T) {
// 	mockConn := &MockWebSocketConn{}
// 	mockDialer := &websocket.Dialer{
// 		HandshakeTimeout: 5 * time.Second,
// 	}

// 	client := &WebSocketClient{
// 		dialer: mockDialer, // Use the mock dialer in WebSocketClient
// 		conn:   mockConn,   // Use mock WebSocket connection
// 	}

// 	opts := ConnectionOptions{
// 		URL:     URLOptions{Scheme: WS, Host: "example.com", Paths: []string{"/ws"}},
// 		Timeout: 5 * time.Second,
// 	}

// 	err := client.Connect(opts)
// 	assert.Nil(t, err, "Expected no error while connecting WebSocket client")
// }

// func TestWebSocketClientConnectFailure(t *testing.T) {
// 	mockDialer := &websocket.Dialer{
// 		HandshakeTimeout: 5 * time.Second,
// 	}

// 	client := &WebSocketClient{
// 		dialer: mockDialer,
// 	}

// 	opts := ConnectionOptions{
// 		URL:     URLOptions{Scheme: WS, Host: "example.com", Paths: []string{"/ws"}},
// 		Timeout: 5 * time.Second,
// 	}

// 	// Simulate failure in connection
// 	mockDialer = &websocket.Dialer{}
// 	err := client.Connect(opts)
// 	assert.NotNil(t, err, "Expected error while connecting WebSocket client")
// }

// func TestWebSocketClientSendSuccess(t *testing.T) {
// 	mockConn := &MockWebSocketConn{}
// 	client := &WebSocketClient{
// 		conn: mockConn,
// 	}

// 	err := client.Send(websocket.TextMessage, []byte("Hello"))
// 	assert.Nil(t, err, "Expected no error while sending message")
// 	assert.Equal(t, websocket.TextMessage, mockConn.WrittenMessageType, "Expected message type to be TextMessage")
// 	assert.Equal(t, []byte("Hello"), mockConn.WrittenMessage, "Expected message content to be 'Hello'")
// }

// func TestWebSocketClientSendError(t *testing.T) {
// 	client := &WebSocketClient{
// 		conn: nil, // Simulate no active WebSocket connection
// 	}

// 	err := client.Send(websocket.TextMessage, []byte("Hello"))
// 	assert.NotNil(t, err, "Expected error while sending message")
// 	assert.Equal(t, "no WebSocket connection available", err.Error(), "Expected no WebSocket connection error")
// }

// func TestWebSocketClientReceiveSuccess(t *testing.T) {
// 	mockConn := &MockWebSocketConn{
// 		ReadMessageType: websocket.TextMessage,
// 		ReadMessage:     []byte("Hello from WebSocket"),
// 		ReadError:       nil,
// 	}

// 	client := &WebSocketClient{
// 		conn: mockConn,
// 	}

// 	messageType, message, err := client.Receive()
// 	assert.Nil(t, err, "Expected no error while receiving message")
// 	assert.Equal(t, websocket.TextMessage, messageType, "Expected message type to be TextMessage")
// 	assert.Equal(t, []byte("Hello from WebSocket"), message, "Expected received message content to be 'Hello from WebSocket'")
// }

// func TestWebSocketClientReceiveError(t *testing.T) {
// 	mockConn := &MockWebSocketConn{
// 		ReadError: errors.New("failed to read message"),
// 	}

// 	client := &WebSocketClient{
// 		conn: mockConn,
// 	}

// 	_, _, err := client.Receive()
// 	assert.NotNil(t, err, "Expected error while receiving message")
// 	assert.Equal(t, "failed to read message", err.Error(), "Expected failed to read message error")
// }

// func TestWebSocketClientReconnect(t *testing.T) {
// 	mockConn := &MockWebSocketConn{}
// 	mockDialer := &websocket.Dialer{
// 		HandshakeTimeout: 5 * time.Second,
// 	}

// 	client := &WebSocketClient{
// 		dialer: mockDialer,
// 		conn:   mockConn, // Use mock WebSocket connection
// 	}

// 	opts := ConnectionOptions{
// 		URL:     URLOptions{Scheme: WS, Host: "example.com", Paths: []string{"/ws"}},
// 		Timeout: 5 * time.Second,
// 	}

// 	err := client.Connect(opts)
// 	assert.Nil(t, err, "Expected no error while connecting WebSocket client")

// 	err = client.Reconnect()
// 	assert.Nil(t, err, "Expected no error while reconnecting WebSocket client")
// }

// func TestWebSocketClientClose(t *testing.T) {
// 	mockConn := &MockWebSocketConn{}

// 	client := &WebSocketClient{
// 		conn: mockConn, // Use mock WebSocket connection
// 	}

// 	err := client.Close()
// 	assert.Nil(t, err, "Expected no error while closing WebSocket connection")
// }
