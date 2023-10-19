package autopaho

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/internal/testserver"
	"github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho"
	paholog "github.com/eclipse/paho.golang/paho/log"
	"github.com/eclipse/paho.golang/paho/session/state"
)

//
// This file contains tests that focus on confirming that queued messages are delivered
//

// TestQueuedMessages attempts to send 100 messages before the connection comes up and then 100 more afterwards (with
// disconnects during the process).
func TestQueuedMessages(t *testing.T) {
	t.Parallel()
	broker, _ := url.Parse(dummyURL)
	serverLogger := paholog.NewTestLogger(t, "testServer:")
	logger := paholog.NewTestLogger(t, "test:")

	ts := testserver.New(serverLogger)
	got200Messages := make(chan struct{}) // Closed when 200 messages received
	var receivedPublish []*packets.Publish
	lastDup := 0 // Position in receivedPublish of the last duplicate we received

	ts.SetPacketReceivedCallback(func(cp *packets.ControlPacket) error {
		pub, ok := cp.Content.(*packets.Publish)
		if !ok {
			return nil
		}
		if pub.Duplicate { // Ignore duplicates if we have received them previously and they are in order
			for i, msg := range receivedPublish {
				if bytes.Compare(msg.Payload, pub.Payload) == 0 {
					if i >= lastDup {
						lastDup = i // ignoring i<lastDup means out of order messages will result in out of order receivedPublish
						return nil
					}
				}
			}
		}
		receivedPublish = append(receivedPublish, pub)
		if l := len(receivedPublish); l == 200 {
			close(got200Messages)
		} else if l%20 == 0 {
			return fmt.Errorf("disconnecting every 20 messages") // Test interaction of queue and store
		}
		return nil
	})

	// We expect messages
	pahoConnUpChan := make(chan struct{}) // Closed first time autopaho reports connection is up

	var allowConnection atomic.Bool

	// custom session because we don't want the client to close it when the connection is lost
	var tsDone chan struct{} // Set on AttemptConnection and closed when that test server connection is done
	session := state.NewInMemory()
	session.SetErrorLogger(paholog.NewTestLogger(t, "sessionError:"))
	session.SetDebugLogger(paholog.NewTestLogger(t, "sessionDebug:"))
	defer session.Close()
	connectCount := 0
	config := ClientConfig{
		BrokerUrls:        []*url.URL{broker},
		KeepAlive:         60,
		ConnectRetryDelay: 500 * time.Millisecond, // Retry connection very quickly!
		ConnectTimeout:    shortDelay,             // Connection should come up very quickly
		AttemptConnection: func(ctx context.Context, _ ClientConfig, _ *url.URL) (net.Conn, error) {
			if !allowConnection.Load() {
				return nil, fmt.Errorf("some random error")
			}
			var conn net.Conn
			var err error
			conn, tsDone, err = ts.Connect(ctx)
			return conn, err
		},
		OnConnectionUp: func(*ConnectionManager, *paho.Connack) {
			connectCount++
			if connectCount == 1 {
				close(pahoConnUpChan)
			}
		},
		Debug:                         logger,
		PahoDebug:                     logger,
		PahoErrors:                    logger,
		CleanStartOnInitialConnection: false, // Want session to stay up (this is the default)
		SessionExpiryInterval:         600,   // If 0 then the state will be removed when the connection drops
		ClientConfig: paho.ClientConfig{
			ClientID: "test",
			Session:  session,
			Router:   paho.NewSingleHandlerRouter(func(publish *paho.Publish) {}),
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), longerDelay)
	defer cancel()
	cm, err := NewConnection(ctx, config)
	if err != nil {
		t.Fatalf("expected NewConnection success: %s", err)
	}
	testFmt := "Test%d"

	// Transmit first 100 messages (should go into queue)
	for i := 1; i <= 100; i++ {
		msg := fmt.Sprintf(testFmt, i)
		if err = cm.PublishViaQueue(ctx, &QueuePublish{
			Publish: &paho.Publish{
				QoS:        1,
				Topic:      msg,
				Properties: nil,
				Payload:    []byte(msg),
			},
		}); err != nil {
			t.Fatalf("publish %d failed", i)
		}
		time.Sleep(time.Millisecond) // for logging
	}

	// Allow connection to come up and wait for this to happen
	allowConnection.Store(true)
	select {
	case <-pahoConnUpChan:
	case <-time.After(shortDelay):
		t.Fatal("timeout awaiting connection up")
	}

	// Transmit another 100 messages
	for i := 101; i <= 200; i++ {
		msg := fmt.Sprintf(testFmt, i)
		if err = cm.PublishViaQueue(ctx, &QueuePublish{
			Publish: &paho.Publish{
				QoS:        1,
				Topic:      msg,
				Properties: nil,
				Payload:    []byte(msg),
			},
		}); err != nil {
			t.Fatalf("publish %d failed", i)
		}
	}

	// Wait for all messages to be received
	select {
	case <-got200Messages:
	case <-time.After(5 * longerDelay):
		t.Fatal("timeout awaiting messages")
	}

	// Disconnect
	disconnectErr := make(chan error)
	go func() {
		disconnectErr <- cm.Disconnect(ctx)
	}()
	select {
	case err = <-disconnectErr:
		if err != nil {
			t.Fatalf("Disconnect returned error: %s", err)
		}
	case <-time.After(longerDelay):
		t.Fatal("Disconnect should return relatively quickly")
	}

	// Connection manager should be Done
	select {
	case <-cm.Done():
	case <-time.After(shortDelay):
		t.Fatal("connection manager should be done after Disconnect Called")
	}

	// The test server should have picked up the dropped connection
	select {
	case <-tsDone:
	case <-time.After(shortDelay):
		t.Fatal("test server did not shutdown within expected time")
	}

	// Check that we received the expected messages, in the expected order
	for i := 1; i <= 200; i++ {
		exp := fmt.Sprintf(testFmt, i)
		if string(receivedPublish[i-1].Payload) != exp {
			t.Errorf("expected %s, got %s", exp, receivedPublish[i-1])
		}
	}
}
