package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	_ "github.com/joho/godotenv/autoload"
	"github.com/the-protobuf-project/runtime-go/network"
)

func main() {
	fmt.Println("=== WebSocket Client Examples ===")
	fmt.Println("Using WebSocket Echo Server")

	// Create WebSocket client
	netConn, err := network.NewConnection(network.WebsocketConnClient)
	if err != nil {
		log.Fatalf("Failed to create network connection: %v", err)
	}
	defer func() { _ = netConn.Close() }()

	// Example 1: Simple echo connection
	example1_SimpleEcho(netConn)

	// Example 2: Auto-reconnection
	example2_AutoReconnection(netConn)

	// Example 3: Graceful shutdown
	example3_GracefulShutdown(netConn)
}

// Example 1: Simple echo connection
func example1_SimpleEcho(netConn *network.Network) {
	fmt.Println("1. Simple WebSocket Echo")
	fmt.Println("   Connecting to wss://echo.websocket.org")

	opts := network.ConnectionOptions{
		URL: network.URLOptions{
			Scheme: network.WSS,
			Host:   "echo.websocket.org",
			Paths:  []string{"/"},
		},
		Timeout: 10 * time.Second,
	}

	_, err := netConn.WithOpts(opts)
	if err != nil {
		log.Printf("   ERROR: Failed to connect: %v\n\n", err)
		return
	}

	wsClient, err := netConn.AsWebSocketConnectionType()
	if err != nil {
		log.Printf("   ERROR: Failed to cast to WebSocket client: %v\n\n", err)
		return
	}

	fmt.Println("   SUCCESS: Connected!")

	// Send a message
	message := []byte("Hello, WebSocket!")
	fmt.Printf("   Sending: %s\n", string(message))
	err = wsClient.Send(websocket.TextMessage, message)
	if err != nil {
		log.Printf("   ERROR: Failed to send message: %v\n\n", err)
		return
	}

	// Receive echo response
	msgType, response, err := wsClient.Receive()
	if err != nil {
		log.Printf("   ERROR: Failed to receive message: %v\n\n", err)
		return
	}

	fmt.Printf("   Received (type %d): %s\n", msgType, string(response))
	fmt.Println("   SUCCESS: Echo successful!")

	// Close connection
	_ = wsClient.Close()
}

// Example 2: Auto-reconnection
func example2_AutoReconnection(netConn *network.Network) {
	fmt.Println("2. WebSocket with Auto-Reconnection")
	fmt.Println("   Connecting to wss://echo.websocket.org")

	opts := network.ConnectionOptions{
		URL: network.URLOptions{
			Scheme: network.WSS,
			Host:   "echo.websocket.org",
			Paths:  []string{"/"},
		},
		Timeout: 10 * time.Second,
	}

	_, err := netConn.WithOpts(opts)
	if err != nil {
		log.Printf("   ERROR: Failed to connect: %v\n\n", err)
		return
	}

	wsClient, err := netConn.AsWebSocketConnectionType()
	if err != nil {
		log.Printf("   ERROR: Failed to cast to WebSocket client: %v\n\n", err)
		return
	}

	// Enable auto-reconnection
	wsClient.SetAutoReconnect(true, 3*time.Second)
	fmt.Println("   SUCCESS: Connected with auto-reconnect enabled!")

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	messageCount := 0

	// Start listening for messages
	errChan := wsClient.Listen(ctx, func(messageType int, message []byte) {
		messageCount++
		fmt.Printf("    Message %d received: %s\n", messageCount, string(message))
	})

	// Send some test messages
	for i := 1; i <= 3; i++ {
		msg := fmt.Sprintf("Message #%d from client", i)
		err := wsClient.Send(websocket.TextMessage, []byte(msg))
		if err != nil {
			log.Printf("   WARNING:  Failed to send message %d: %v\n", i, err)
		} else {
			fmt.Printf("    Sent: %s\n", msg)
		}
		time.Sleep(1 * time.Second)
	}

	// Wait for listen to complete or error
	select {
	case err := <-errChan:
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			log.Printf("   WARNING:  Listen error: %v\n", err)
		}
	case <-ctx.Done():
		fmt.Println("     Context timeout reached")
	}

	fmt.Printf("   SUCCESS: Received %d messages total\n\n", messageCount)
	_ = wsClient.Close()
}

// Example 3: Graceful shutdown with signal handling
func example3_GracefulShutdown(netConn *network.Network) {
	fmt.Println("3. WebSocket with Graceful Shutdown")
	fmt.Println("   Connecting to wss://echo.websocket.org")
	fmt.Println("   Press Ctrl+C to trigger graceful shutdown")

	opts := network.ConnectionOptions{
		URL: network.URLOptions{
			Scheme: network.WSS,
			Host:   "echo.websocket.org",
			Paths:  []string{"/"},
		},
		Timeout: 10 * time.Second,
	}

	_, err := netConn.WithOpts(opts)
	if err != nil {
		log.Printf("   ERROR: Failed to connect: %v\n\n", err)
		return
	}

	wsClient, err := netConn.AsWebSocketConnectionType()
	if err != nil {
		log.Printf("   ERROR: Failed to cast to WebSocket client: %v\n\n", err)
		return
	}

	fmt.Println("   SUCCESS: Connected!")

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Create context that can be cancelled
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	messageCount := 0

	// Start listening
	errChan := wsClient.Listen(ctx, func(messageType int, message []byte) {
		messageCount++
		fmt.Printf("    Message %d: %s\n", messageCount, string(message))
	})

	// Send messages in a goroutine
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		msgNum := 1
		for {
			select {
			case <-ticker.C:
				msg := fmt.Sprintf("Periodic message #%d", msgNum)
				err := wsClient.Send(websocket.TextMessage, []byte(msg))
				if err != nil {
					log.Printf("   WARNING:  Failed to send: %v\n", err)
					return
				}
				fmt.Printf("    Sent: %s\n", msg)
				msgNum++
			case <-ctx.Done():
				return
			}
		}
	}()

	// Wait for either signal or error
	select {
	case <-sigChan:
		fmt.Println("\n    Shutdown signal received")
		cancel() // Cancel context to stop listening
		time.Sleep(500 * time.Millisecond)
		fmt.Println("   SUCCESS: Graceful shutdown complete")
	case err := <-errChan:
		if err != nil {
			log.Printf("   WARNING:  Connection error: %v\n", err)
		}
	case <-time.After(10 * time.Second):
		fmt.Println("\n     Demo timeout reached")
		cancel()
	}

	fmt.Printf("    Total messages received: %d\n\n", messageCount)
	_ = wsClient.Close()
}
