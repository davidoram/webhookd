package main

import (
	"crypto/tls"
	"net/http"
	"time"
)

// AuthTokenRoundTripper is a custom RoundTripper that enforces TLS and adds the Authorization header to every request.
type AuthTokenRoundTripper struct {
	Transport    http.RoundTripper
	SharedSecret string
	Timeout      time.Duration
}

// RoundTrip is the implementation of the RoundTripper interface.
func (a *AuthTokenRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	// Set the Authorization header for every request
	req.Header.Set("Authorization", "Bearer "+a.SharedSecret)

	// If the Transport is not set, use the default transport
	if a.Transport == nil {
		// Use a custom transport with TLS configuration
		tlsConfig := &tls.Config{
			InsecureSkipVerify: false, // Set to true if you want to skip server certificate verification (not recommended for production)
		}

		transport := &http.Transport{
			TLSClientConfig: tlsConfig,
		}

		return transport.RoundTrip(req)
	}

	// Use the specified transport to perform the request
	return a.Transport.RoundTrip(req)
}

// NewAuthTokenClient creates a new HTTP client that will send a set token in the 'Authorization' header
// It will also enforce TLS verification, and accept a client side timeout
func NewAuthTokenClient(sharedSecret string, timeout time.Duration) *http.Client {
	return &http.Client{
		Transport: &AuthTokenRoundTripper{
			Transport:    http.DefaultTransport,
			SharedSecret: sharedSecret,
		},
		Timeout: timeout,
	}
}
