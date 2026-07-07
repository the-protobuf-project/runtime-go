// HTTP request execution, retries, and status handling.
package network

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

// HTTPResponse holds the result of an HTTP request: raw body bytes and an error
// (from a network failure or a non-2xx status). On retry exhaustion both may be set.
type HTTPResponse struct {
	Data  []byte
	Error error
}

// Request performs an HTTP request asynchronously with optional retries. It builds the
// URL from urlOptions and pathIndex, sends the request, and retries up to maxRetries
// times with RetryDelay between attempts. The returned channel receives exactly one
// HTTPResponse and is closed. Context cancellation aborts the request and any retries.
func (h *HTTPClient) Request(ctx context.Context, method HTTPMethod, urlOptions URLOptions, body []byte, headers map[string]string, pathIndex, maxRetries int) <-chan HTTPResponse {
	resultChan := make(chan HTTPResponse, 1)
	go func() {
		defer close(resultChan)

		retryDelay := h.RetryDelay
		if retryDelay == 0 {
			retryDelay = 2 * time.Second
		}

		var lastErr error
		for attempt := 0; attempt <= maxRetries; attempt++ {
			if attempt > 0 {
				select {
				case <-time.After(retryDelay):
				case <-ctx.Done():
					resultChan <- HTTPResponse{Error: ctx.Err()}
					return
				}
			}

			requestCtx, cancel := context.WithTimeout(ctx, h.Timeout)
			fullURL, err := buildFullURL(urlOptions, pathIndex)
			if err != nil {
				cancel()
				resultChan <- HTTPResponse{Error: fmt.Errorf("failed to build URL: %w", err)}
				return
			}
			req, err := h.buildRequest(requestCtx, method, fullURL, body, headers)
			if err != nil {
				cancel()
				resultChan <- HTTPResponse{Error: fmt.Errorf("failed to build request: %w", err)}
				return
			}
			data, err := h.sendRequest(req)
			cancel()
			if err == nil {
				resultChan <- HTTPResponse{Data: data}
				return
			}
			lastErr = err
		}
		resultChan <- HTTPResponse{Error: fmt.Errorf("request failed after %d retries: %w", maxRetries, lastErr)}
	}()
	return resultChan
}

// RequestSync is a blocking version of Request that returns the body and error.
func (h *HTTPClient) RequestSync(ctx context.Context, method HTTPMethod, urlOptions URLOptions, body []byte, headers map[string]string, pathIndex, maxRetries int) ([]byte, error) {
	response := <-h.Request(ctx, method, urlOptions, body, headers, pathIndex, maxRetries)
	return response.Data, response.Error
}

// buildRequest allocates an HTTP request with the given method, URL, body, and headers.
func (h *HTTPClient) buildRequest(ctx context.Context, method HTTPMethod, fullURL string, body []byte, headers map[string]string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, string(method), fullURL, bytes.NewBuffer(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	return req, nil
}

// sendRequest performs the request, reads the body, and validates the status code.
// Returns the response body and an error for non-2xx status codes.
func (h *HTTPClient) sendRequest(req *http.Request) ([]byte, error) {
	resp, err := h.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error making request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}
	if err := validateStatusCode(resp.StatusCode); err != nil {
		return data, err // return body alongside the error for inspection
	}
	return data, nil
}

// validateStatusCode maps HTTP status codes to errors. 2xx returns nil; 4xx/5xx
// return descriptive errors.
func validateStatusCode(statusCode int) error {
	switch {
	case statusCode >= 200 && statusCode < 300:
		return nil
	case statusCode == 400:
		return fmt.Errorf("client error: status code %d (Bad Request)", statusCode)
	case statusCode == 401:
		return fmt.Errorf("client error: status code %d (Unauthorized)", statusCode)
	case statusCode == 403:
		return fmt.Errorf("client error: status code %d (Forbidden)", statusCode)
	case statusCode == 404:
		return fmt.Errorf("client error: status code %d (Not Found)", statusCode)
	case statusCode == 429:
		return fmt.Errorf("client error: status code %d (Too Many Requests)", statusCode)
	case statusCode >= 500 && statusCode < 600:
		return fmt.Errorf("server error: status code %d", statusCode)
	default:
		return fmt.Errorf("unexpected status code: %d", statusCode)
	}
}
