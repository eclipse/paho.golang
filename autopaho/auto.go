package autopaho

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/eclipse/paho.golang/paho"
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

// WebSocketConfig enables customisation of the websocket connection
type WebSocketConfig struct {
	Dialer func(url *url.URL, tlsCfg *tls.Config) *websocket.Dialer // If non-nil this will be called before each websocket connection (allows full configuration of the dialer used)
	Header func(url *url.URL, tlsCfg *tls.Config) http.Header       // If non-nil this will be called before each connection attempt to get headers to include with request
}

// ClientConfig adds a few values, required to manage the connection, to the standard paho.ClientConfig (note that
// conn will be ignored)
type ClientConfig struct {
	BrokerUrls        []*url.URL       // URL(s) for the broker (schemes supported include 'mqtt' and 'tls')
	TlsCfg            *tls.Config      // Configuration used when connecting using TLS
	KeepAlive         uint16           // Keepalive period in seconds (the maximum time interval that is permitted to elapse between the point at which the Client finishes transmitting one MQTT Control Packet and the point it starts sending the next)
	ConnectRetryDelay time.Duration    // How long to wait between connection attempts (defaults to 10s)
	ConnectTimeout    time.Duration    // How long to wait for the connection process to complete (defaults to 10s)
	WebSocketCfg      *WebSocketConfig // Enables customisation of the websocket connection

	OnConnectionUp func(*ConnectionManager, *paho.Connack) // Called (within a goroutine) when a connection is made (including reconnection). Connection Manager passed to simplify subscriptions.
	OnConnectError func(error)                             // Called (within a goroutine) whenever a connection attempt fails

	Debug      paho.Logger // By default set to NOOPLogger{},set to a logger for debugging info
	PahoDebug  paho.Logger // debugger passed to the paho package (will default to NOOPLogger{})
	PahoErrors paho.Logger // error logger passed to the paho package (will default to NOOPLogger{})

	connectUsername string
	connectPassword []byte

	willTopic           string
	willPayload         []byte
	willQos             byte
	willRetain          bool
	willPayloadFormat   byte
	willMessageExpiry   uint32
	willContentType     string
	willResponseTopic   string
	willCorrelationData []byte

	connectPacketBuilder func(*paho.Connect) *paho.Connect

	// We include the full paho.ClientConfig in order to simplify moving between the two packages.
	// Note that Conn will be ignored.
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

// ResetUsernamePassword clears any configured username and password on the client configuration
func (cfg *ClientConfig) ResetUsernamePassword() {
	cfg.connectPassword = []byte{}
	cfg.connectUsername = ""
}

// SetUsernamePassword configures username and password properties for the Connect packets
// These values are staged in the ClientConfig, and preparation of the Connect packet is deferred.
func (cfg *ClientConfig) SetUsernamePassword(username string, password []byte) {
	if len(username) > 0 {
		cfg.connectUsername = username
	}

	if len(password) > 0 {
		cfg.connectPassword = password
	}
}

// SetWillMessage configures the Will topic, payload, QOS and Retain facets of the client connection
// These values are staged in the ClientConfig, for later preparation of the Connect packet.
func (cfg *ClientConfig) SetWillMessage(topic string, payload []byte, qos byte, retain bool) {
	cfg.willTopic = topic
	cfg.willPayload = payload
	cfg.willQos = qos
	cfg.willRetain = retain
}

// SetConnectPacketConfigurator assigns a callback for modification of the Connect packet, called before the connection is opened, allowing the application to adjust its configuration before establishing a connection.
// This function should be treated as asynchronous, and expected to have no side effects.
func (cfg *ClientConfig) SetConnectPacketConfigurator(fn func(*paho.Connect) *paho.Connect) bool {
	cfg.connectPacketBuilder = fn
	return fn != nil
}

// buildConnectPacket constructs a Connect packet for the paho client, based on staged configuration.
// If the program uses SetConnectPacketConfigurator, the provided callback will be executed with the preliminary Connect packet representation.
func (cfg *ClientConfig) buildConnectPacket() *paho.Connect {

	cp := &paho.Connect{
		KeepAlive:  cfg.KeepAlive,
		ClientID:   cfg.ClientID,
		CleanStart: true, // while persistence is not supported we should probably start clean...
	}

	if len(cfg.connectUsername) > 0 {
		cp.UsernameFlag = true
		cp.Username = cfg.connectUsername
	}

	if len(cfg.connectPassword) > 0 {
		cp.PasswordFlag = true
		cp.Password = cfg.connectPassword
	}

	if len(cfg.willTopic) > 0 && len(cfg.willPayload) > 0 {
		cp.WillMessage = &paho.WillMessage{
			Retain:  cfg.willRetain,
			Payload: cfg.willPayload,
			Topic:   cfg.willTopic,
			QoS:     cfg.willQos,
		}

		// how the broker should wait before considering the client disconnected
		// hopefully this default is sensible for most applications, tolerating short interruptions
		willDelayInterval := uint32(2 * cfg.KeepAlive)

		cp.WillProperties = &paho.WillProperties{
			// Most of these are nil/empty or defaults until related methods are exposed for configuration
			WillDelayInterval: &willDelayInterval,
			PayloadFormat:     &cfg.willPayloadFormat,
			MessageExpiry:     &cfg.willMessageExpiry,
			ContentType:       cfg.willContentType,
			ResponseTopic:     cfg.willResponseTopic,
			CorrelationData:   cfg.willCorrelationData,
		}
	}

	if nil != cfg.connectPacketBuilder {
		cp = cfg.connectPacketBuilder(cp)
	}

	return cp
}

// NewConnection creates a connection manager and begins the connection process (will retry until the context is cancelled)
func NewConnection(ctx context.Context, cfg ClientConfig) (*ConnectionManager, error) {
	if cfg.Debug == nil {
		cfg.Debug = paho.NOOPLogger{}
	}
	if cfg.ConnectRetryDelay == 0 {
		cfg.ConnectRetryDelay = 10 * time.Second
	}
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = 10 * time.Second
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

	mainLoop:
		for {
			// Error handler is used to guarantee that a single error will be received whenever the connection is lost
			eh := errorHandler{
				debug:                  cfg.Debug,
				mu:                     sync.Mutex{},
				errChan:                errChan,
				userOnClientError:      cfg.OnClientError,
				userOnServerDisconnect: cfg.OnServerDisconnect,
			}
			cliCfg := cfg
			cliCfg.OnClientError = eh.onClientError
			cliCfg.OnServerDisconnect = eh.onServerDisconnect

			cli, connAck := establishBrokerConnection(innerCtx, cliCfg)
			if cli == nil {
				break mainLoop // Only occurs when context is cancelled
			}
			c.mu.Lock()
			c.cli = cli
			c.mu.Unlock()
			close(c.connUp)

			if cfg.PahoDebug != nil {
				cli.SetDebugLogger(cfg.PahoDebug)
			}

			if cfg.PahoErrors != nil {
				cli.SetErrorLogger(cfg.PahoErrors)
			}

			if cfg.OnConnectionUp != nil {
				cfg.OnConnectionUp(&c, connAck)
			}

			var err error
			select {
			case err = <-errChan: // Message on error channel indicates connection has (or will) drop.
			case <-innerCtx.Done():
				// As the connection is up we call disconnect to shut things down cleanly
				if err = c.cli.Disconnect(&paho.Disconnect{ReasonCode: 0}); err != nil {
					cfg.Debug.Printf("disconnect returned error: %s\n", err)
				}
				if ctx.Err() != nil { // If this is due to outer context being cancelled then this will have happened before the inner one gets cancelled.
					cfg.Debug.Printf("broker connection handler exiting due to context: %s\n", ctx.Err())
				} else {
					cfg.Debug.Printf("broker connection handler exiting due to Disconnect call: %s\n", innerCtx.Err())
				}
				break mainLoop
			}
			c.mu.Lock()
			c.cli = nil
			c.connUp = make(chan struct{})
			c.mu.Unlock()
			cfg.Debug.Printf("connection to broker lost (%s); will reconnect\n", err)
		}
		cfg.Debug.Println("connection manager has terminated")
	}()
	return &c, nil
}

// Disconnect closes the connection (if one is up) and shuts down any active processes before returning
// Note: We cannot currently tell when the mqtt has fully shutdown (so it may still be in the process of closing down)
func (c *ConnectionManager) Disconnect(ctx context.Context) error {
	c.cancelCtx()
	select {
	case <-c.done: // wait for goroutine to exit
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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
