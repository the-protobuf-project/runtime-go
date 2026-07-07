package main

import (
	"context"
	"fmt"
	"log"
	"time"

	_ "github.com/joho/godotenv/autoload"
	"github.com/the-protobuf-project/runtime-go/network"
)

func main() {
	fmt.Println("=== HTTP Client Examples ===")

	// Create HTTP client
	netConn, err := network.NewConnection(network.HTTPConnClient)
	if err != nil {
		log.Fatalf("Failed to create network connection: %v", err)
	}
	defer func() { _ = netConn.Close() }()

	// Example 1: Simple GET request
	example1_SimpleGET(netConn)

	// Example 2: GET with query parameters
	example2_GETWithParams(netConn)

	// Example 3: POST request with JSON body
	example3_POSTRequest(netConn)

	// Example 4: Request with retries
	example4_RequestWithRetries(netConn)

	// Example 5: Context cancellation
	example5_ContextCancellation(netConn)

	// Example 6: Synchronous request
	example6_SynchronousRequest(netConn)
}

// Example 1: Simple GET request
func example1_SimpleGET(netConn *network.Network) {
	fmt.Println("1. Simple GET Request")
	fmt.Println("   URL: https://httpbin.org/get")

	opts := network.ConnectionOptions{
		URL: network.URLOptions{
			Scheme: network.HTTPS,
			Host:   "httpbin.org",
			Paths:  []string{"/get"},
		},
		Timeout: 10 * time.Second,
	}

	_, err := netConn.WithOpts(opts)
	if err != nil {
		log.Printf("   ERROR: Failed to connect: %v\n", err)
		return
	}

	httpClient, err := netConn.AsHTTPConnectionType()
	if err != nil {
		log.Printf("   ERROR: Failed to cast to HTTP client: %v\n", err)
		return
	}

	ctx := context.Background()
	respChan := httpClient.Request(ctx, network.GET, opts.URL, nil, nil, 0, 0)
	resp := <-respChan

	if resp.Error != nil {
		log.Printf("   ERROR: Request failed: %v\n", resp.Error)
		return
	}

	fmt.Printf("   SUCCESS: Success! Response length: %d bytes\n", len(resp.Data))
	fmt.Printf("   Response preview: %.100s...\n\n", string(resp.Data))
}

// Example 2: GET with query parameters
func example2_GETWithParams(netConn *network.Network) {
	fmt.Println("2. GET Request with Query Parameters")
	fmt.Println("   URL: https://httpbin.org/get?name=John&age=30")

	opts := network.ConnectionOptions{
		URL: network.URLOptions{
			Scheme: network.HTTPS,
			Host:   "httpbin.org",
			Paths:  []string{"/get"},
			Params: map[string]string{
				"name": "John",
				"age":  "30",
			},
		},
		Timeout: 10 * time.Second,
	}

	_, err := netConn.WithOpts(opts)
	if err != nil {
		log.Printf("   ERROR: Failed to connect: %v\n", err)
		return
	}

	httpClient, _ := netConn.AsHTTPConnectionType()
	ctx := context.Background()
	respChan := httpClient.Request(ctx, network.GET, opts.URL, nil, nil, 0, 0)
	resp := <-respChan

	if resp.Error != nil {
		log.Printf("   ERROR: Request failed: %v\n", resp.Error)
		return
	}

	fmt.Printf("   SUCCESS: Success! Response length: %d bytes\n\n", len(resp.Data))
}

// Example 3: POST request with JSON body
func example3_POSTRequest(netConn *network.Network) {
	fmt.Println("3. POST Request with JSON Body")
	fmt.Println("   URL: https://httpbin.org/post")

	opts := network.ConnectionOptions{
		URL: network.URLOptions{
			Scheme: network.HTTPS,
			Host:   "httpbin.org",
			Paths:  []string{"/post"},
		},
		Timeout: 10 * time.Second,
	}

	_, err := netConn.WithOpts(opts)
	if err != nil {
		log.Printf("   ERROR: Failed to connect: %v\n", err)
		return
	}

	httpClient, _ := netConn.AsHTTPConnectionType()

	// JSON body
	jsonBody := []byte(`{"name": "John Doe", "email": "john@example.com", "age": 30}`)

	// Headers
	headers := map[string]string{
		"Content-Type": "application/json",
		"Accept":       "application/json",
	}

	ctx := context.Background()
	respChan := httpClient.Request(ctx, network.POST, opts.URL, jsonBody, headers, 0, 0)
	resp := <-respChan

	if resp.Error != nil {
		log.Printf("   ERROR: Request failed: %v\n", resp.Error)
		return
	}

	fmt.Printf("   SUCCESS: Success! Response length: %d bytes\n", len(resp.Data))
	fmt.Printf("   Response preview: %.100s...\n\n", string(resp.Data))
}

// Example 4: Request with retries
func example4_RequestWithRetries(netConn *network.Network) {
	fmt.Println("4. Request with Automatic Retries")
	fmt.Println("   URL: https://httpbin.org/status/500 (will fail and retry)")

	opts := network.ConnectionOptions{
		URL: network.URLOptions{
			Scheme: network.HTTPS,
			Host:   "httpbin.org",
			Paths:  []string{"/status/500"}, // This endpoint returns 500 error
		},
		Timeout:    10 * time.Second,
		RetryDelay: 1 * time.Second,
	}

	_, err := netConn.WithOpts(opts)
	if err != nil {
		log.Printf("   ERROR: Failed to connect: %v\n", err)
		return
	}

	httpClient, _ := netConn.AsHTTPConnectionType()

	ctx := context.Background()
	maxRetries := 2
	fmt.Printf("   Attempting request with %d retries...\n", maxRetries)

	respChan := httpClient.Request(ctx, network.GET, opts.URL, nil, nil, 0, maxRetries)
	resp := <-respChan

	if resp.Error != nil {
		fmt.Printf("   SUCCESS: Expected failure after retries: %v\n\n", resp.Error)
		return
	}

	fmt.Printf("   Response: %d bytes\n\n", len(resp.Data))
}

// Example 5: Context cancellation
func example5_ContextCancellation(netConn *network.Network) {
	fmt.Println("5. Request with Context Cancellation")
	fmt.Println("   URL: https://httpbin.org/delay/5 (5 second delay)")

	opts := network.ConnectionOptions{
		URL: network.URLOptions{
			Scheme: network.HTTPS,
			Host:   "httpbin.org",
			Paths:  []string{"/delay/5"},
		},
		Timeout: 30 * time.Second,
	}

	_, err := netConn.WithOpts(opts)
	if err != nil {
		log.Printf("   ERROR: Failed to connect: %v\n", err)
		return
	}

	httpClient, _ := netConn.AsHTTPConnectionType()

	// Create context with 2 second timeout
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	fmt.Println("   Sending request with 2s timeout (server delays 5s)...")
	respChan := httpClient.Request(ctx, network.GET, opts.URL, nil, nil, 0, 0)
	resp := <-respChan

	if resp.Error != nil {
		fmt.Printf("   SUCCESS: Expected timeout: %v\n\n", resp.Error)
		return
	}

	fmt.Printf("   Unexpected success: %d bytes\n\n", len(resp.Data))
}

// Example 6: Synchronous request
func example6_SynchronousRequest(netConn *network.Network) {
	fmt.Println("6. Synchronous Request (Blocking)")
	fmt.Println("   URL: https://httpbin.org/uuid")

	opts := network.ConnectionOptions{
		URL: network.URLOptions{
			Scheme: network.HTTPS,
			Host:   "httpbin.org",
			Paths:  []string{"/uuid"},
		},
		Timeout: 10 * time.Second,
	}

	_, err := netConn.WithOpts(opts)
	if err != nil {
		log.Printf("   ERROR: Failed to connect: %v\n", err)
		return
	}

	httpClient, _ := netConn.AsHTTPConnectionType()

	ctx := context.Background()
	data, err := httpClient.RequestSync(ctx, network.GET, opts.URL, nil, nil, 0, 0)

	if err != nil {
		log.Printf("   ERROR: Request failed: %v\n", err)
		return
	}

	fmt.Printf("   SUCCESS: Success! Response: %s\n", string(data))
}
