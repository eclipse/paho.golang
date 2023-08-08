// build +unittest

package autopaho

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/eclipse/paho.golang/autopaho/internal/testserver"
	"github.com/eclipse/paho.golang/paho"
)

const shortDelay = 500 * time.Millisecond // Used when something should happen pretty quickly (increase when debugging)
const longerDelay = time.Second           // Longer delay than above (for things like test wide context timeout)

// When debugging uncomment the below (to prevent tests terminating too quickly)
// const shortDelay = time.Hour
// const longerDelay = time.Hour

const dummyURL = "tcp://127.0.0.1:1883"

// TestMain customised to verify no goroutine leaks (easy to introduce into the code!)
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// TestReconnect confirms that the connection is automatically re-established when lost
func TestReconnect(t *testing.T) {
	broker, _ := url.Parse(dummyURL)
	serverLogger := &testLog{l: t, prefix: "testServer:"}
	logger := &testLog{l: t, prefix: "test:"}
	defer func() {
		// Prevent any logging after completion. Unfortunately, there is currently no way to know if paho.Client
		// has fully shutdown. As such, messages may be logged after shutdown (which will result in a panic).
		serverLogger.Stop()
		logger.Stop()
	}()

	ts := testserver.New(serverLogger)

	type tsConnUpMsg struct {
		cancelFn func()        // Function to cancel test broker context
		done     chan struct{} // Will be closed when the test broker has disconnected (and shutdown)
	}
	tsConnUpChan := make(chan tsConnUpMsg) // Message will be sent when test broker connection is up
	pahoConnUpChan := make(chan struct{})  // When autopaho reports connection is up write to channel will occur

	atCount := 0

	config := ClientConfig{
		BrokerUrls:        []*url.URL{broker},
		KeepAlive:         60,
		ConnectRetryDelay: time.Millisecond, // Retry connection very quickly!
		ConnectTimeout:    shortDelay,       // Connection should come up very quickly
		AttemptConnection: func(ctx context.Context, _ ClientConfig, _ *url.URL) (net.Conn, error) {
			atCount += 1
			if atCount == 2 { // fail on the initial reconnection attempt to exercise retry functionality
				return nil, errors.New("connection attempt failed")
			}
			ctx, cancel := context.WithCancel(ctx)
			conn, done, err := ts.Connect(ctx)
			if err == nil { // The above may fail if attempted too quickly (before disconnect processed)
				tsConnUpChan <- tsConnUpMsg{cancelFn: cancel, done: done}
			} else {
				cancel()
			}
			return conn, err
		},
		OnConnectionUp: func(*ConnectionManager, *paho.Connack) { pahoConnUpChan <- struct{}{} },
		Debug:          logger,
		PahoDebug:      logger,
		PahoErrors:     logger,
		ClientConfig: paho.ClientConfig{
			ClientID: "test",
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cm, err := NewConnection(ctx, config)
	if err != nil {
		t.Fatalf("expected NewConnection success: %s", err)
	}

	var initialConnUpMsg tsConnUpMsg
	select {
	case initialConnUpMsg = <-tsConnUpChan:
	case <-time.After(shortDelay):
		t.Fatal("timeout awaiting initial connection request")
	}
	select {
	case <-pahoConnUpChan:
	case <-time.After(shortDelay):
		t.Fatal("timeout awaiting connection up")
	}

	// Force a disconnect
	initialConnUpMsg.cancelFn()
	select {
	case <-initialConnUpMsg.done:
	case <-time.After(shortDelay):
		t.Fatal("timeout awaiting test broker shutdown")
	}

	// Await reconnection
	var secondConnUpMsg tsConnUpMsg
	select {
	case secondConnUpMsg = <-tsConnUpChan:
	case <-time.After(shortDelay):
		t.Fatal("timeout awaiting reconnection request")
	}
	select {
	case <-pahoConnUpChan:
	case <-time.After(shortDelay):
		t.Fatal("timeout awaiting reconnection up")
	}

	// Clean shutdown
	cancel() // Cancelling outer context will cascade

	select {
	case <-secondConnUpMsg.done:
	case <-time.After(shortDelay):
		t.Fatal("timeout awaiting second test broker shutdown")
	}
	select {
	case <-cm.Done():
	case <-time.After(shortDelay):
		t.Fatal("timeout awaiting connection manager shutdown")
	}

	// Prevent any future logging - unfortunately, there is currently no way to know if paho.Client has completely
	// shutdown. As such, messages may be logged after shutdown (which will result in a panic).
	serverLogger.Stop()
	logger.Stop()
}

// TestBasicPubSub performs pub/sub operations at each QOS level
func TestBasicPubSub(t *testing.T) {
	broker, _ := url.Parse(dummyURL)
	serverLogger := &testLog{l: t, prefix: "testServer:"}
	logger := &testLog{l: t, prefix: "test:"}
	defer func() {
		// Prevent any logging after completion. Unfortunately, there is currently no way to know if paho.Client
		// has fully shutdown. As such, messages may be logged after shutdown (which will result in a panic).
		serverLogger.Stop()
		logger.Stop()
	}()

	ts := testserver.New(serverLogger)

	type tsConnUpMsg struct {
		cancelFn func()        // Function to cancel test broker context
		done     chan struct{} // Will be closed when the test broker has disconnected (and shutdown)
	}
	tsConnUpChan := make(chan tsConnUpMsg) // Message will be sent when test broker connection is up
	pahoConnUpChan := make(chan struct{})  // When autopaho reports connection is up write to channel will occur

	const expectedMessages = 3
	var mrMu sync.Mutex
	mrDone := make(chan struct{}) // Closed when the expected messages have been received
	var messagesReceived []*paho.Publish

	atCount := 0

	config := ClientConfig{
		BrokerUrls:        []*url.URL{broker},
		KeepAlive:         60,
		ConnectRetryDelay: time.Millisecond, // Retry connection very quickly!
		ConnectTimeout:    shortDelay,       // Connection should come up very quickly
		AttemptConnection: func(ctx context.Context, _ ClientConfig, _ *url.URL) (net.Conn, error) {
			atCount += 1
			if atCount > 1 { // force failure if a reconnection is attempted (the connection should not drop in this test)
				return nil, errors.New("connection attempt failed")
			}
			ctx, cancel := context.WithCancel(ctx)
			conn, done, err := ts.Connect(ctx)
			if err == nil { // The above may fail if attempted too quickly (before disconnect processed)
				tsConnUpChan <- tsConnUpMsg{cancelFn: cancel, done: done}
			} else {
				cancel()
			}
			logger.Println("connection up")
			return conn, err
		},
		OnConnectionUp: func(*ConnectionManager, *paho.Connack) { pahoConnUpChan <- struct{}{} },
		Debug:          logger,
		PahoDebug:      logger,
		PahoErrors:     logger,
		ClientConfig: paho.ClientConfig{
			ClientID: "test",
			Router: paho.NewSingleHandlerRouter(func(publish *paho.Publish) {
				mrMu.Lock()
				defer mrMu.Unlock()
				messagesReceived = append(messagesReceived, publish)
				if len(messagesReceived) == expectedMessages {
					close(mrDone)
				}
			}),
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), longerDelay)
	defer cancel()
	cm, err := NewConnection(ctx, config)
	if err != nil {
		t.Fatalf("expected NewConnection success: %s", err)
	}

	// Wait for connection to come up
	var initialConnUpMsg tsConnUpMsg
	select {
	case initialConnUpMsg = <-tsConnUpChan:
	case <-time.After(shortDelay):
		t.Fatal("timeout awaiting initial connection request")
	}
	select {
	case <-pahoConnUpChan:
	case <-time.After(shortDelay):
		t.Fatal("timeout awaiting connection up")
	}

	testFmt := "Test%d"
	var subs []paho.SubscribeOptions
	for i := 0; i < 3; i++ {
		subs = append(subs, paho.SubscribeOptions{
			Topic: fmt.Sprintf(testFmt, i),
			QoS:   byte(i),
		})
	}
	if _, err = cm.Subscribe(ctx, &paho.Subscribe{
		Subscriptions: subs,
	}); err != nil {
		t.Fatalf("subscribe failed: %s", err)
	}

	// Note: The client does not currently support
	for i := 0; i < 3; i++ {
		t.Logf("publish QOS %d message", i)
		msg := fmt.Sprintf(testFmt, i)
		if _, err := cm.Publish(ctx, &paho.Publish{
			QoS:        byte(i),
			Topic:      msg,
			Properties: nil,
			Payload:    []byte(msg),
		}); err != nil {
			t.Fatalf("publish at QOS %d failed: %s", i, err)
		}
		t.Logf("publish QOS %d message complete", i)
	}

	// Wait until we have received the expected messages
	select {
	case <-mrDone:
	case <-time.After(shortDelay):
		mrMu.Lock()
		t.Fatalf("received %d of the expected %d messages (%v)", len(messagesReceived), expectedMessages, messagesReceived)
		// mrMu.Unlock() not needed as Fatal exits
	}

	// Note: While messages have been received, the QOS2 handshake process is probably still in progress
	// (router callback is called before any acknowledgement is sent).
	t.Log("messages received - closing connection")
	cancel() // Cancelling outer context will cascade
	select { // Wait for the local client to terminate
	case <-cm.Done():
	case <-time.After(shortDelay):
		t.Fatal("timeout awaiting connection manager shutdown")
	}

	select { // Wait for test server to terminate
	case <-initialConnUpMsg.done:
	case <-time.After(shortDelay):
		t.Fatal("test server did not shut down in a timely manner")
	}

	// Check we got what we expected
	for i := 0; i < 3; i++ {
		msg := fmt.Sprintf(testFmt, i)
		if messagesReceived[i].Topic != msg {
			t.Errorf("expected topic %s, got %s", msg, messagesReceived[i].Topic)
		}
		if string(messagesReceived[i].Payload) != msg {
			t.Errorf("expected message %v, got %v", []byte(msg), messagesReceived[i].Payload)
		}
		if err != nil {
			t.Fatalf("publish at QOS %d failed: %s", i, err)
		}
	}
}

// TestClientConfig_buildConnectPacket exercises buildConnectPacket checking that options and callbacks are applied
func TestClientConfig_buildConnectPacket(t *testing.T) {
	broker, _ := url.Parse(dummyURL)

	config := ClientConfig{
		BrokerUrls:        []*url.URL{broker},
		KeepAlive:         5,
		ConnectRetryDelay: 5 * time.Second,
		ConnectTimeout:    5 * time.Second,

		// extends the lower-level paho.ClientConfig
		ClientConfig: paho.ClientConfig{
			ClientID: "test",
		},
	}

	// Validate initial state
	cp := config.buildConnectPacket()

	if cp.WillMessage != nil {
		t.Errorf("Expected empty Will message, got: %v", cp.WillMessage)
	}

	if cp.UsernameFlag != false || cp.Username != "" {
		t.Errorf("Expected absent/empty username, got: flag=%v username=%v", cp.UsernameFlag, cp.Username)
	}

	if cp.PasswordFlag != false || len(cp.Password) > 0 {
		t.Errorf("Expected absent/empty password, got: flag=%v password=%v", cp.PasswordFlag, cp.Password)
	}

	// Set some common parameters
	config.SetUsernamePassword("testuser", []byte("testpassword"))
	config.SetWillMessage(fmt.Sprintf("client/%s/state", config.ClientID), []byte("disconnected"), 1, true)

	cp = config.buildConnectPacket()

	if cp.UsernameFlag == false || cp.Username != "testuser" {
		t.Errorf("Expected a username, got: flag=%v username=%v", cp.UsernameFlag, cp.Username)
	}

	pMatch := bytes.Compare(cp.Password, []byte("testpassword"))

	if cp.PasswordFlag == false || len(cp.Password) == 0 || pMatch != 0 {
		t.Errorf("Expected a password, got: flag=%v password=%v", cp.PasswordFlag, cp.Password)
	}

	if cp.WillMessage == nil {
		t.Error("Expected a Will message, found nil")
	}

	if cp.WillMessage.Topic != "client/test/state" {
		t.Errorf("Will message topic did not match expected [%v], found [%v]", "client/test/state", cp.WillMessage.Topic)
	}

	if cp.WillMessage.QoS != byte(1) {
		t.Errorf("Will message QOS did not match expected [1]: found [%v]", cp.WillMessage.QoS)
	}

	if cp.WillMessage.Retain != true {
		t.Errorf("Will message Retain did not match expected [true]: found [%v]", cp.WillMessage.Retain)
	}

	if *(cp.WillProperties.WillDelayInterval) != 10 { // assumes default 2x keep alive
		t.Errorf("Will message Delay Interval did not match expected [10]: found [%v]", *(cp.Properties.WillDelayInterval))
	}

	// Set an override method for the CONNECT packet
	config.SetConnectPacketConfigurator(func(c *paho.Connect) *paho.Connect {
		delay := uint32(200)
		c.WillProperties.WillDelayInterval = &delay
		return c
	})

	cp = config.buildConnectPacket()

	if *(cp.WillProperties.WillDelayInterval) != 200 { // verifies the override
		t.Errorf("Will message Delay Interval did not match expected [200]: found [%v]", *(cp.Properties.WillDelayInterval))
	}

}

// testLogger contains the logging functions provided by testing.T
type testLogger interface {
	Log(args ...interface{})
	Logf(format string, args ...interface{})
}

// The testLog type is an adapter to allow the use of testing.T as a paho.Logger.
// With this implementation, log messages will only be output when a test fails (and will be associated with the test).
type testLog struct {
	sync.Mutex
	l      testLogger
	prefix string
}

// Println prints a line to the log
// Println its arguments in the test log (only printed if the test files or appropriate arguments passed to go test).
func (t *testLog) Println(v ...interface{}) {
	t.Lock()
	defer t.Unlock()
	if t.l != nil {
		t.l.Log(append([]interface{}{t.prefix}, v...)...)
	}
}

// Printf formats its arguments according to the format, analogous to fmt.Printf, and
// records the text in the test log (only printed if the test files or appropriate arguments passed to go test).
func (t *testLog) Printf(format string, v ...interface{}) {
	t.Lock()
	defer t.Unlock()
	if t.l != nil {
		t.l.Logf(t.prefix+format, v...)
	}
}

// Stop prevents future logging
func (t *testLog) Stop() {
	t.Lock()
	defer t.Unlock()
	t.l = nil
}
