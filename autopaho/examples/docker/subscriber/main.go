package main

// Connect to the broker, subscribe, and write messages received to a file

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ChIoT-Tech/paho.golang/autopaho"
	"github.com/ChIoT-Tech/paho.golang/paho"
)

const (
	topic             = "topic1"
	qos               = 1
	serverURL         = "tcp://mosquitto:1883"
	connectRetryDelay = 10 * time.Second
	clientID          = "mqtt_subscriber"
	keepAlive         = 30 // seconds

	writeToLog  = true  // If true then received messages will be written to the console
	writeToDisk = false // If true then received messages will be written to the file below
	outputFile  = "/binds/receivedMessages.txt"

	debug = true // autopaho and paho debug output requested
)

func main() {
	// Create a handler that will deal with incoming messages
	h := NewHandler(writeToDisk, outputFile, writeToLog)
	defer h.Close()

	// Enable logging by uncommenting the below
	su, err := url.Parse(serverURL)
	if err != nil {
		panic(err)
	}

	cfg := autopaho.ClientConfig{
		BrokerUrls:        []*url.URL{su},
		KeepAlive:         keepAlive,
		ConnectRetryDelay: connectRetryDelay,
		OnConnectionUp: func(cm *autopaho.ConnectionManager, connAck *paho.Connack) {
			fmt.Println("mqtt connection up")
			if _, err := cm.Subscribe(context.Background(), &paho.Subscribe{
				Subscriptions: map[string]paho.SubscribeOptions{
					topic: {QoS: qos},
				},
			}); err != nil {
				fmt.Printf("failed to subscribe (%s). This is likely to mean no messages will be received.", err)
				return
			}
			fmt.Println("mqtt subscription made")
		},
		OnConnectError: func(err error) { fmt.Printf("error whilst attempting connection: %s\n", err) },
		ClientConfig: paho.ClientConfig{
			ClientID: clientID,
			Router: paho.NewSingleHandlerRouter(func(m *paho.Publish) {
				h.handle(m)
			}),
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

	//
	// Connect to the broker
	//
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cm, err := autopaho.NewConnection(ctx, cfg)
	if err != nil {
		panic(err)
	}

	// Messages will be handled through the callback so we really just need to wait until a shutdown
	// is requested
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	signal.Notify(sig, syscall.SIGTERM)

	<-sig
	fmt.Println("signal caught - exiting")

	// We could cancel the context at this point but will call Disconnect instead (this waits for autopaho to shutdown)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	cm.Disconnect(ctx)

	fmt.Println("shutdown complete")
}

// subscribe will attempt to subscribe to the nominated topic
func subscribe(mqtt *autopaho.ConnectionManager, topic string, qos byte) error {
	if _, err := mqtt.Subscribe(context.Background(), &paho.Subscribe{
		Subscriptions: map[string]paho.SubscribeOptions{
			topic: {QoS: qos},
		},
	}); err != nil {
		return err
	}
	return nil
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
