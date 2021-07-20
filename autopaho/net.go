package autopaho

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/eclipse/paho.golang/paho"
)

// Network (establishing connection) functionality for AutoPaho

// establishBrokerConnection - establishes a connection with the broker retrying until successful or the
// context is cancelled (in which case nil will be returned).
func establishBrokerConnection(ctx context.Context, cfg ClientConfig) (*paho.Client, *paho.Connack) {
	// Note: We do not touch b.cli in order to avoid adding thread safety issues.
	var err error

	for {
		for _, u := range cfg.BrokerUrls {
			connectionCtx, cancelConnCtx := context.WithTimeout(ctx, cfg.ConnectTimeout)

			switch strings.ToLower(u.Scheme) {
			case "mqtt", "tcp", "":
				cfg.Conn, err = attemptTCPConnection(connectionCtx, u.Host)
			case "ssl", "tls", "mqtts", "mqtt+ssl", "tcps":
				cfg.Conn, err = attemptTLSConnection(connectionCtx, cfg.TlsCfg, u.Host)
			case "ws":
				cfg.Conn, err = attemptWebsocketConnection(connectionCtx, nil, cfg.WebSocketCfg, u)
			case "wss":
				cfg.Conn, err = attemptWebsocketConnection(connectionCtx, cfg.TlsCfg, cfg.WebSocketCfg, u)
			default:
				cfg.OnConnectError(fmt.Errorf("unsupported scheme (%s) user in url %s", u.Scheme, u.String()))
				cancelConnCtx()
				continue
			}

			if err == nil {
				cli := paho.NewClient(cfg.ClientConfig)
				cp := cfg.buildConnectPacket()
				var ca *paho.Connack
				ca, err = cli.Connect(connectionCtx, cp) // will return an error if the connection is unsuccessful (checks the reason code)
				if err == nil {                          // Successfully connected
					cancelConnCtx()
					return cli, ca
				}
			}
			cancelConnCtx()

			// Possible failure was due to outer context being cancelled
			if ctx.Err() != nil {
				return nil, nil
			}

			if cfg.OnConnectError != nil {
				cfg.OnConnectError(fmt.Errorf("failed to connect to %s: %w", u.String(), err))
			}
		}

		// Delay before attempting another connection
		select {
		case <-time.After(cfg.ConnectRetryDelay):
		case <-ctx.Done():
			return nil, nil
		}
	}
}

// attemptTCPConnection - makes a single attempt at establishing a TCP connection with the broker
func attemptTCPConnection(ctx context.Context, address string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, "tcp", address)
}

// attemptTLSConnection - makes a single attempt at establishing a TLS connection with the broker
func attemptTLSConnection(ctx context.Context, tlsCfg *tls.Config, address string) (net.Conn, error) {
	d := tls.Dialer{
		Config: tlsCfg,
	}
	return d.DialContext(ctx, "tcp", address)
}

// attemptWebsocketConnection - makes a single attempt at establishing a websocket connection with the broker
func attemptWebsocketConnection(ctx context.Context, tlsc *tls.Config, cfg *WebSocketConfig, brokerURL *url.URL) (net.Conn, error) {
	var dialer *websocket.Dialer
	var requestHeader http.Header
	if cfg != nil {
		if cfg.Dialer != nil {
			dialer = cfg.Dialer(brokerURL, tlsc)
		}
		if cfg.Header != nil {
			requestHeader = cfg.Header(brokerURL, tlsc)
		}
	}
	if dialer == nil {
		d := *websocket.DefaultDialer // Take a copy as we modify a few values
		d.TLSClientConfig = tlsc
		d.Subprotocols = []string{"mqtt"}
		dialer = &d
	}
	ws, _, err := dialer.DialContext(ctx, brokerURL.String(), requestHeader)

	if err != nil {
		return nil, fmt.Errorf("websocket connection failed: %w", err)
	}

	wrapper := &websocketConnector{
		Conn: ws,
	}
	return wrapper, err
}

// websocketConnector is a websocket wrapper so it satisfies the net.Conn interface so it is a
// drop in replacement of the golang.org/x/net/websocket package.
// Implementation guide taken from https://github.com/gorilla/websocket/issues/282
type websocketConnector struct {
	*websocket.Conn
	r   io.Reader
	rio sync.Mutex
	wio sync.Mutex
}

// SetDeadline sets both the read and write deadlines
// Note: deadlines are fatal in websocket connections (so this does not really match net.Conn)
func (c *websocketConnector) SetDeadline(t time.Time) error {
	if err := c.SetReadDeadline(t); err != nil {
		return err
	}
	err := c.SetWriteDeadline(t)
	return err
}

// Write writes data to the websocket
func (c *websocketConnector) Write(p []byte) (int, error) {
	c.wio.Lock()
	defer c.wio.Unlock()

	err := c.WriteMessage(websocket.BinaryMessage, p)
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

// Read reads the current websocket frame
func (c *websocketConnector) Read(p []byte) (int, error) {
	c.rio.Lock()
	defer c.rio.Unlock()
	for {
		if c.r == nil {
			// Advance to next message.
			var err error
			_, c.r, err = c.NextReader()
			if err != nil {
				return 0, err
			}
		}
		n, err := c.r.Read(p)
		if err == io.EOF {
			// At end of message.
			c.r = nil
			if n > 0 {
				return n, nil
			}
			// No data read, continue to next message.
			continue
		}
		return n, err
	}
}
