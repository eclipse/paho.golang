package autopaho

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/ChIoT-Tech/paho.golang/paho"
)

// Network (establishing connection) functionality for AutoPaho

// establishBrokerConnection - establishes a connection with the broker retrying until successful or the
// context is cancelled (in which case nil will be returned).
func establishBrokerConnection(ctx context.Context, cfg ClientConfig) (*paho.Client, *paho.Connack) {
	// Note: We do not touch b.cli in order to avoid adding thread safety issues.
	var err error

	for {
		for _, u := range cfg.BrokerUrls {
			switch strings.ToLower(u.Scheme) {
			case "mqtt", "tcp", "":
				cfg.Conn, err = attemptTCPConnection(ctx, u.Host)
			case "ssl", "tls", "mqtts", "mqtt+ssl", "tcps":
				cfg.Conn, err = attemptTLSConnection(ctx, cfg.TlsCfg, u.Host)
			default:
				cfg.OnConnectError(fmt.Errorf("unsupported scheme (%s) user in url %s", u.Scheme, u.String()))
				continue
			}

			if err == nil {
				cli := paho.NewClient(cfg.ClientConfig)
				cp := &paho.Connect{
					KeepAlive:  cfg.KeepAlive,
					ClientID:   cfg.ClientID,
					CleanStart: true,
				}
				ca, err := cli.Connect(ctx, cp) // will return an error if the connection is unsuccessful (checks the reason code)
				if err == nil {                 // Successfully connected
					return cli, ca
				}
			}
			// Possible failure was due to context being cancelled
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
