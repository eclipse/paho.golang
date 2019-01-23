package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"

	"github.com/eclipse/paho.golang/paho"
	"github.com/eclipse/paho.golang/paho/extensions/rpc"
)

func listener(server, rTopic, username, password string) {
	conn, err := net.Dial("tcp", server)
	if err != nil {
		log.Fatalf("Failed to connect to %s: %s", server, err)
	}

	c := paho.NewClient()
	c.Router = paho.NewSingleHandlerRouter(func(m *paho.Publish) {
		if m.Properties != nil && m.Properties.CorrelationData != nil && m.Properties.ResponseTopic != "" {
			log.Printf("Received message with response topic %s and correl id %s\n%s", m.Properties.ResponseTopic, string(m.Properties.CorrelationData), string(m.Payload))
			c.Publish(context.Background(), &paho.Publish{
				Properties: &paho.PublishProperties{
					CorrelationData: m.Properties.CorrelationData,
				},
				Topic:   m.Properties.ResponseTopic,
				Payload: []byte(`{"response":"bar"}`),
			})
		}
	})
	c.Conn = conn

	cp := &paho.Connect{
		KeepAlive:  30,
		CleanStart: true,
		Username:   username,
		Password:   []byte(password),
	}

	if username != "" {
		cp.UsernameFlag = true
	}
	if password != "" {
		cp.PasswordFlag = true
	}

	ca, err := c.Connect(context.Background(), cp)
	if err != nil {
		log.Fatalln(err)
	}
	if ca.ReasonCode != 0 {
		log.Fatalf("Failed to connect to %s : %d - %s", server, ca.ReasonCode, ca.Properties.ReasonString)
	}

	fmt.Printf("Connected to %s\n", server)

	c.Subscribe(context.Background(), &paho.Subscribe{
		Subscriptions: map[string]paho.SubscribeOptions{
			rTopic: paho.SubscribeOptions{QoS: 0},
		},
	})
}

func main() {
	server := flag.String("server", "127.0.0.1:1883", "The full URL of the MQTT server to connect to")
	rTopic := flag.String("rtopic", "rpc/request", "Topic for requests to go to")
	clientid := flag.String("clientid", "", "A clientid for the requestor connection")
	username := flag.String("username", "", "A username to authenticate to the MQTT server")
	password := flag.String("password", "", "Password to match username")
	flag.Parse()

	listener(*server, *rTopic, *username, *password)

	conn, err := net.Dial("tcp", *server)
	if err != nil {
		log.Fatalf("Failed to connect to %s: %s", *server, err)
	}

	c := paho.NewClient()
	c.Router = paho.NewSingleHandlerRouter(nil)
	c.Conn = conn

	cp := &paho.Connect{
		KeepAlive:  30,
		ClientID:   *clientid,
		CleanStart: true,
		Username:   *username,
		Password:   []byte(*password),
	}

	if *username != "" {
		cp.UsernameFlag = true
	}
	if *password != "" {
		cp.PasswordFlag = true
	}

	ca, err := c.Connect(context.Background(), cp)
	if err != nil {
		log.Fatalln(err)
	}
	if ca.ReasonCode != 0 {
		log.Fatalf("Failed to connect to %s : %d - %s", *server, ca.ReasonCode, ca.Properties.ReasonString)
	}

	fmt.Printf("Connected to %s\n", *server)

	h, err := rpc.NewHandler(c)
	if err != nil {
		log.Fatal(err)
	}

	resp, err := h.Request(&paho.Publish{
		Payload: []byte(`'{"request":"foo"}`),
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Received response: %s", string(resp.Payload))
}
