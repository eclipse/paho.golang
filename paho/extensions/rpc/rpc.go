package rpc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/eclipse/paho.golang/paho"
)

type Publisher interface {
	Publish(context.Context, *paho.Publish) (*paho.PublishResponse, error)
}

type Subscriber interface {
	Subscribe(context.Context, *paho.Subscribe) (*paho.Suback, error)
	Unsubscribe(context.Context, *paho.Unsubscribe) (*paho.Unsuback, error)
}

type PubSubClient interface {
	Publisher
	Subscriber
	UseClient(func(c *paho.Client) error) error
	GetClientID() string
}

// Handler is the struct providing a request/response functionality for the paho
// MQTT v5 client
type Handler struct {
	sync.Mutex
	c          PubSubClient
	correlData map[string]chan *paho.Publish
}

func NewHandler(cm PubSubClient) (*Handler, error) {
	h := &Handler{
		c:          cm,
		correlData: make(map[string]chan *paho.Publish),
	}

	cm.UseClient(func(c *paho.Client) error {
		c.Router.RegisterHandler(fmt.Sprintf("%s/responses", c.ClientID), h.responseHandler)
		return nil
	})

	clientID := cm.GetClientID()

	_, err := cm.Subscribe(context.Background(), &paho.Subscribe{
		Subscriptions: map[string]paho.SubscribeOptions{
			fmt.Sprintf("%s/responses", clientID): {QoS: 1},
		},
	})
	if err != nil {
		return nil, err
	}

	return h, nil
}

func (h *Handler) addCorrelID(cID string, r chan *paho.Publish) {
	h.Lock()
	defer h.Unlock()

	h.correlData[cID] = r
}

func (h *Handler) getCorrelIDChan(cID string) chan *paho.Publish {
	h.Lock()
	defer h.Unlock()

	rChan := h.correlData[cID]
	delete(h.correlData, cID)

	return rChan
}

func (h *Handler) Request(pb *paho.Publish) (*paho.Publish, error) {
	cID := fmt.Sprintf("%d", time.Now().UnixNano())
	rChan := make(chan *paho.Publish)

	h.addCorrelID(cID, rChan)

	if pb.Properties == nil {
		pb.Properties = &paho.PublishProperties{}
	}

	pb.Properties.CorrelationData = []byte(cID)
	pb.Properties.ResponseTopic = fmt.Sprintf("%s/responses", h.c.GetClientID())
	pb.Retain = false

	_, err := h.c.Publish(context.Background(), pb)
	if err != nil {
		return nil, err
	}

	resp := <-rChan
	return resp, nil
}

func (h *Handler) responseHandler(pb *paho.Publish) {
	if pb.Properties == nil || pb.Properties.CorrelationData == nil {
		return
	}

	rChan := h.getCorrelIDChan(string(pb.Properties.CorrelationData))
	if rChan == nil {
		return
	}

	rChan <- pb
}
