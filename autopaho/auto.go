package autopaho

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/ChIoT-Tech/paho.golang/paho"
)

// AutoPaho is a wrapper around github.com/eclipse/paho.golang that simplifies the connection process; it automates
// connections (retrying until the connection comes up) and will attempt to re-establish the connection if it is lost.
//
// The aim is to cover a common requirement (connect to the broker and try to keep the connection up); if your
// requirements differ then please consider using github.com/eclipse/paho.golang directly (perhaps using the
// code in this file as a base; a secondary aim is to provide example code!).

// ConnectionDownError Down will be returned when a request is made but the connection to the broker is down
// Note: It is possible that the connection will drop between the request being made and a response being received in
// which case a different error will be received (this is only returned if the connection is down at the time the
// request is made).
var ConnectionDownError = errors.New("connection with the MQTT broker is currently down")

// ClientConfig adds a few values, required to manage the connection, to the standard paho.ClientConfig (note that
// conn will be ignored)
type ClientConfig struct {
	BrokerUrls        []*url.URL    // URL(s) for the broker (schemes supported include 'mqtt' and 'tls')
	TlsCfg            *tls.Config   // Configuration used when connecting using TLS
	KeepAlive         uint16        // Keepalive period in seconds (the maximum time interval that is permitted to elapse between the point at which the Client finishes transmitting one MQTT Control Packet and the point it starts sending the next)
	ConnectRetryDelay time.Duration // How long to wait between connection attempts

	OnConnectionUp func(*ConnectionManager, *paho.Connack) // Called (within a goroutine) when a connection is made (including reconnection). Connection Manager passed to simplify subscriptions.
	OnConnectError func(error)                             // Called (within a goroutine) whenever a connection attempt fails

	Debug paho.Logger // NOOPLogger{},

	// We include the full paho.ClientConfig in order to simplify moving between the two packages.
	// Note that that Conn will be ignored.
	paho.ClientConfig
}

// ConnectionManager manages the connection with the broker and provides thew ability to publish messages
type ConnectionManager struct {
	cli    *paho.Client  // The client will only be set when the connection is up (only updated within NewBrokerConnection goRoutine)
	connUp chan struct{} // Channel is closed when the connection is up
	mu     sync.Mutex    // protects both of the above

	cancelCtx context.CancelFunc // Calling this will shut things down cleanly

	done chan struct{} // Channel that will be closed when the process has cleanly shutdown
}

// NewConnection creates a connection manager and begins the connection process (will retry until the context is cancelled)
func NewConnection(ctx context.Context, cfg ClientConfig) (*ConnectionManager, error) {
	if cfg.Debug == nil {
		cfg.Debug = paho.NOOPLogger{}
	}

	innerCtx, cancel := context.WithCancel(ctx)
	c := ConnectionManager{
		cli:       nil,
		connUp:    make(chan struct{}),
		cancelCtx: cancel,
		done:      make(chan struct{}),
	}
	errChan := make(chan error)

	go func() {
		defer close(c.done)

		for {
			eh := errorHandler{debug: cfg.Debug, errChan: errChan, userFun: cfg.OnClientError} // We want a single error to come through when the connection fails
			cliCfg := cfg.ClientConfig
			cliCfg.OnClientError = eh.onClientError
			cli, connAck := establishBrokerConnection(ctx, cfg)
			if cli == nil {
				return // Only occurs when context is cancelled
			}
			c.mu.Lock()
			c.cli = cli
			c.mu.Unlock()
			close(c.connUp)

			if cfg.OnConnectionUp != nil {
				cfg.OnConnectionUp(&c, connAck)
			}

			var err error
			select {
			case err = <-errChan: // Message on error channel indicates connection has (or will) drop.
			case <-innerCtx.Done():
				// As the connection is up we call disconnect to shut things down cleanly
				if err = c.cli.Disconnect(&paho.Disconnect{ReasonCode: 0}); err != nil {
					cfg.Debug.Printf("disconnect returned error: %s", err)
				}
				if ctx.Err() != nil { // If this is due to outer context being cancelled then this will have happened before the inner one gets cancelled.
					cfg.Debug.Printf("broker connection handler exiting due to context: %s", ctx.Err())
				} else {
					cfg.Debug.Printf("broker connection handler exiting due to Disconnect call: %s", innerCtx.Err())
				}
				return
			}
			c.mu.Lock()
			c.cli = nil
			c.connUp = make(chan struct{})
			c.mu.Unlock()
			cfg.Debug.Printf("connection to broker lost (%s); will reconnect", err)
		}
	}()
	cfg.Debug.Println("connection manager has terminated")
	return &c, nil
}

// Disconnect closes the connection (if one is up) and shuts down any active processes before returning
// Note: We cannot currently tell when the mqtt has fully shutdown (so it may still be in the process of closing down)
func (c *ConnectionManager) Disconnect() {
	c.cancelCtx()
	<-c.done // wait for goroutine to exit
}

// Done returns a channel that will be closed when the connection handler has shutdown cleanly
// Note: We cannot currently tell when the mqtt has fully shutdown (so it may still be in the process of closing down)
func (c *ConnectionManager) Done() <-chan struct{} {
	return c.done
}

// AwaitConnection will return when the connection comes up or the context is cancelled (only returns an error
// if context is cancelled). If you require more complex connection management then consider using the OnConnectionUp
// callback.
func (c *ConnectionManager) AwaitConnection(ctx context.Context) error {
	c.mu.Lock()
	ch := c.connUp
	c.mu.Unlock()

	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done: // If connection process is cancelled we should exit
		return fmt.Errorf("connection manager shutting down")
	}
}

// Subscribe is used to send a Subscription request to the MQTT server.
// It is passed a pre-prepared Subscribe packet and blocks waiting for
// a response Suback, or for the timeout to fire. Any response Suback
// is returned from the function, along with any errors.
func (c *ConnectionManager) Subscribe(ctx context.Context, s *paho.Subscribe) (*paho.Suback, error) {
	c.mu.Lock()
	cli := c.cli
	c.mu.Unlock()

	if cli == nil {
		return nil, ConnectionDownError
	}
	return cli.Subscribe(ctx, s)
}

// Unsubscribe is used to send an Unsubscribe request to the MQTT server.
// It is passed a pre-prepared Unsubscribe packet and blocks waiting for
// a response Unsuback, or for the timeout to fire. Any response Unsuback
// is returned from the function, along with any errors.
func (c *ConnectionManager) Unsubscribe(ctx context.Context, u *paho.Unsubscribe) (*paho.Unsuback, error) {
	c.mu.Lock()
	cli := c.cli
	c.mu.Unlock()

	if cli == nil {
		return nil, ConnectionDownError
	}
	return cli.Unsubscribe(ctx, u)
}

// Publish is used to send a publication to the MQTT server.
// It is passed a pre-prepared Publish packet and blocks waiting for
// the appropriate response, or for the timeout to fire.
// Any response message is returned from the function, along with any errors.
func (c *ConnectionManager) Publish(ctx context.Context, p *paho.Publish) (*paho.PublishResponse, error) {
	c.mu.Lock()
	cli := c.cli
	c.mu.Unlock()

	if cli == nil {
		return nil, ConnectionDownError
	}
	return cli.Publish(ctx, p)
}
