package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/autopaho/extensions/rpc"
	"github.com/eclipse/paho.golang/paho"
)

func init() {
	ic := make(chan os.Signal, 1)
	signal.Notify(ic, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ic
		os.Exit(0)
	}()
}

type Request struct {
	Function string `json:"function"`
	Param1   int    `json:"param1"`
	Param2   int    `json:"param2"`
}

type Response struct {
	Value int `json:"value"`
}

func listener(rTopic string) {
	var v sync.WaitGroup

	v.Add(1)

	go func() {

		cfg, err := getConfig()
		if err != nil {
			panic(err)
		}
		cfg.clientID = "listen1"

		cliCfg := getCmConfig(cfg)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		cm, err := autopaho.NewConnection(ctx, cliCfg)
		if err != nil {
			panic(err)
		}

		time.Sleep(5 * time.Second)

		cliCfg.Router.RegisterHandler(rTopic, func(m *paho.Publish) {
			if m.Properties != nil && m.Properties.CorrelationData != nil && m.Properties.ResponseTopic != "" {
				log.Printf("Received message with response topic %s and correl id %s\n%s", m.Properties.ResponseTopic, string(m.Properties.CorrelationData), string(m.Payload))

				var r Request
				var resp Response

				if err := json.NewDecoder(bytes.NewReader(m.Payload)).Decode(&r); err != nil {
					log.Printf("Failed to decode Request: %v", err)
				}

				switch r.Function {
				case "add":
					resp.Value = r.Param1 + r.Param2
				case "mul":
					resp.Value = r.Param1 * r.Param2
				case "div":
					resp.Value = r.Param1 / r.Param2
				case "sub":
					resp.Value = r.Param1 - r.Param2
				}

				body, _ := json.Marshal(resp)
				_, err := cm.Publish(ctx, &paho.Publish{
					Properties: &paho.PublishProperties{
						CorrelationData: m.Properties.CorrelationData,
					},
					Topic:   m.Properties.ResponseTopic,
					Payload: body,
				})
				if err != nil {
					log.Fatalf("failed to publish message: %s", err)
				}
			}
		})

		_, err = cm.Subscribe(ctx, &paho.Subscribe{
			Subscriptions: []paho.SubscribeOptions{
				{Topic: rTopic, QoS: 0},
			},
		})
		if err != nil {
			log.Fatalf("failed to subscribe: %s", err)
		}

		v.Done()

		for {
			time.Sleep(1 * time.Second)
		}
	}()

	v.Wait()
}

func main() {
	cfg, err := getConfig()
	if err != nil {
		panic(err)
	}
	cliCfg := getCmConfig(cfg)

	listener(cfg.topic)

	//
	// Connect to the broker
	//
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cm, err := autopaho.NewConnection(ctx, cliCfg)
	if err != nil {
		panic(err)
	}
	log.Print("TEST")

	time.Sleep(5 * time.Second)

	h, err := rpc.NewHandler(ctx, rpc.HandlerOpts{
		Conn:             cm,
		Router:           cliCfg.Router,
		ResponseTopicFmt: "%s/responses",
		ClientID:         cliCfg.ClientID,
	})

	if err != nil {
		log.Fatal(err)
	}

	resp, err := h.Request(ctx, &paho.Publish{
		Topic:   cfg.topic,
		Payload: []byte(`{"function":"mul", "param1": 10, "param2": 5}`),
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Received response: %s", string(resp.Payload))

	resp, err = h.Request(ctx, &paho.Publish{
		Topic:   cfg.topic,
		Payload: []byte(`{"function":"mul", "param1": 10, "param2": 5}`),
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Received response: %s", string(resp.Payload))

	resp, err = h.Request(ctx, &paho.Publish{
		Topic:   cfg.topic,
		Payload: []byte(`{"function":"add", "param1": 5, "param2": 7}`),
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Received response: %s", string(resp.Payload))
}
