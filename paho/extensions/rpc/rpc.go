package rpc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/netdata/paho.golang/paho"
)

// Handler is the struct providing a request/response functionality for the paho
// MQTT v5 client
type Handler struct {
	sync.Mutex
	c          *paho.Client
	clientID   string
	correlData map[string]chan *paho.Publish
}

func NewHandler(config paho.ClientConfig, clientID string) (*Handler, error) {
	r, ok := config.Router.(Router)
	if !ok && config.Router != nil {
		return nil, fmt.Errorf("config.Router must be nil or implement rpc.Router")
	}
	if r == nil {
		r = NewStandardRouter()
		config.Router = r
	}

	c := paho.NewClient(config)
	h := &Handler{
		c:          c,
		correlData: make(map[string]chan *paho.Publish),
		clientID:   clientID,
	}

	r.RegisterHandler(fmt.Sprintf("%s/responses", clientID), h.responseHandler)

	_, err := c.Subscribe(context.Background(), &paho.Subscribe{
		Subscriptions: map[string]paho.SubscribeOptions{
			fmt.Sprintf("%s/responses", clientID): paho.SubscribeOptions{QoS: 1},
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
	pb.Properties.ResponseTopic = fmt.Sprintf("%s/responses", h.clientID)
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
