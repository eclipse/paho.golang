package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/ChIoT-Tech/paho.golang/autopaho"
	"github.com/ChIoT-Tech/paho.golang/paho"
)

// Connect to the broker and publish a message periodically

const (
	topic                = "topic1"
	qos                  = 1
	serverURL            = "tcp://mosquitto:1883"
	connectRetryDelay    = 10 * time.Second
	delayBetweenMessages = time.Second
	clientID             = "mqtt_publisher"
	keepAlive            = 30   // seconds
	printMessages        = true // If true then published messages will be written to the console
	debug                = true // autopaho and paho debug output requested
)

func main() {
	// Enable logging by uncommenting the below
	su, err := url.Parse(serverURL)
	if err != nil {
		panic(err)
	}

	cfg := autopaho.ClientConfig{
		BrokerUrls:        []*url.URL{su},
		KeepAlive:         keepAlive,
		ConnectRetryDelay: connectRetryDelay,
		OnConnectionUp:    func(*autopaho.ConnectionManager, *paho.Connack) { fmt.Println("mqtt connection up") },
		OnConnectError:    func(err error) { fmt.Printf("error whilst attempting connection: %s\n", err) },
		Debug:             paho.NOOPLogger{},
		ClientConfig: paho.ClientConfig{
			ClientID:      clientID,
			OnClientError: func(err error) { fmt.Printf("server requested disconnect: %s\n", err) },
			OnServerDisconnect: func(d *paho.Disconnect) {
				if d.Properties != nil {
					fmt.Printf("server requested disconnect: %s\n", d.Properties.ReasonString)
				} else {
					fmt.Printf("server requested disconnect; reason code: %d\n", d.ReasonCode)
				}
			},
		},
	}

	if debug {
		cfg.Debug = logger{prefix: "autoPaho"}
		cfg.PahoDebug = logger{prefix: "paho"}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Connect to the broker - this will return immediately after initiating the connection process
	cm, err := autopaho.NewConnection(ctx, cfg)
	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup

	// Start off a goRoutine that publishes messages
	wg.Add(1)
	go func() {
		defer wg.Done()
		var count uint64
		for {
			// AwaitConnection will return immediately if connection is up so doing this stops publication whilst connection
			// is unavailable.
			err = cm.AwaitConnection(ctx)
			if err != nil { // Should only happen when context is cancelled
				fmt.Printf("publisher done (AwaitConnection: %s)\n", err)
				return
			}

			count += 1
			// The message could be anything; lets make it JSON containing a simple count (makes it simpler to track the messages)
			msg, err := json.Marshal(struct {
				Count uint64
			}{Count: count})
			if err != nil {
				panic(err)
			}

			// Publish will block so we run it in a goRoutine
			go func(msg []byte) {
				pr, err := cm.Publish(ctx, &paho.Publish{
					QoS:     qos,
					Topic:   topic,
					Payload: msg,
				})
				if err != nil {
					fmt.Printf("error publishing: %s\n", err)
				} else if pr.ReasonCode != 0 && pr.ReasonCode != 16 { // 16 = Server received message but there are no subscribers
					fmt.Printf("reason code %d received\n", pr.ReasonCode)
				} else if printMessages {
					fmt.Printf("sent message: %s\n", msg)
				}
			}(msg)

			select {
			case <-time.After(delayBetweenMessages):
			case <-ctx.Done():
				fmt.Println("publisher done")
				return
			}
		}
	}()

	// Wait for a signal before exiting
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	signal.Notify(sig, syscall.SIGTERM)

	<-sig
	fmt.Println("signal caught - exiting")
	cancel()

	wg.Wait()
	fmt.Println("shutdown complete")
}

// logger implements the paho.Logger interface
type logger struct {
	prefix string
}

// Println is the library provided NOOPLogger's
// implementation of the required interface function()
func (l logger) Println(v ...interface{}) {
	fmt.Println(append([]interface{}{l.prefix + ":"}, v...)...)
}

// Printf is the library provided NOOPLogger's
// implementation of the required interface function(){}
func (l logger) Printf(format string, v ...interface{}) {
	fmt.Printf(l.prefix+":"+format, v...)
}
