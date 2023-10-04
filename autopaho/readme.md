AutoPaho
========

AutoPaho has a number of aims:

* Provide an easy-to-use MQTT v5 client that provides commonly requested functionality (e.g. connection, automatic reconnection).
* Demonstrate the use of `paho.golang/paho`.
* Enable us to smoke test `paho.golang/paho` features (ensuring they are they usable in a real world situation)

## Basic Usage

```Go
package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"syscall"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
)

func main() {
	// App will run until cancelled by user (e.g. ctrl-c)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	u, err := url.Parse("mqtt://test.mosquitto.org:1883")
	if err != nil {
		panic(err)
	}

	cliCfg := autopaho.ClientConfig{
		BrokerUrls: []*url.URL{u},
		KeepAlive:  20, // Keepalive message should be sent every 20 seconds
		OnConnectionUp: func(cm *autopaho.ConnectionManager, connAck *paho.Connack) {
			fmt.Println("mqtt connection up")
			if _, err := cm.Subscribe(context.Background(), &paho.Subscribe{
				Subscriptions: []paho.SubscribeOptions{
					{Topic: "testTopic", QoS: 1},
				},
			}); err != nil {
				fmt.Printf("failed to subscribe (%s). This is likely to mean no messages will be received!\n", err)
			}
			fmt.Println("mqtt subscription made")
		},
		OnConnectError: func(err error) { fmt.Printf("error whilst attempting connection: %s\n", err) },
		// eclipse/paho.golang/paho provides base mqtt functionality, the below config will be passed in for each connection
		ClientConfig: paho.ClientConfig{
			ClientID: "TestClient",
			Router: paho.NewSingleHandlerRouter(func(m *paho.Publish) {
				fmt.Printf("received message on topic %s; body: %s (retain: %t)\n", m.Topic, m.Payload, m.Retain)
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

	c, err := autopaho.NewConnection(ctx, cliCfg)
	if err != nil {
		panic(err)
	}
	// Wait for the connection to come up
	if err = c.AwaitConnection(ctx); err != nil {
		panic(err)
	}
	// Publish a test message
	if _, err = c.Publish(ctx, &paho.Publish{
		QoS:     1,
		Topic:   "testTopic",
		Payload: []byte("TestMessage"),
	}); err != nil {
		panic(err)
	}

	<-ctx.Done() // Wait for user to trigger exit
	fmt.Println("signal caught - exiting")
}
```

## QOS 1 & 2

`paho.golang/paho` supports QOS 1 & 2 but does not maintain a ["Session State" ](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901230). This means that QOS1/2 messages will be sent but no attempt will be made to resend them if there is an issue.

## Work in progress

See [this issue](https://github.com/eclipse/paho.golang/issues/25) for more info (currently nearing completion).

Use case: I want to regularly publish messages and leave it to autopaho to ensure they are delivered (regardless of the connection state).

Acceptance tests:
   * Publish a message before connection is established; it should be queued and delivered when connection comes up.
   * Connection drops and messages are published whilst reconnection is in progress. They should be queued and delivered 
     when connection is available.
   * Publish messages at a rate in excess of Receive Maximum; they should be queued and sent, in order, when possible.
   * Application restarts during any of the above - queued messages are sent out when connection comes up.

Desired features:
   * Fire and forget - async publish; we trust the library to deliver the message so once its in the store the client can forget about it.
   * Minimal RAM use - the connection may be down for a long time and we may not have much ram. So messages should be on disk (ideally with
     no trace of them in RAM)

