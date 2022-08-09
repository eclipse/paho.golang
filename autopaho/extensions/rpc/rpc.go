package rpc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
)

// Handler is the struct providing a request/response functionality for the paho
// MQTT v5 client
type Handler struct {
	sync.Mutex
	cm            *autopaho.ConnectionManager
	correlData    map[string]chan *paho.Publish
	responseTopic string
}

func NewHandler(ctx context.Context, cm *autopaho.ConnectionManager, responseTopic string) (*Handler, error) {
	h := &Handler{
		cm:         cm,
		correlData: make(map[string]chan *paho.Publish),
	}

	var clientId string
	if err := cm.UseClient(func(c *paho.Client) error {
		clientId = c.ClientID
		c.Router.RegisterHandler(fmt.Sprintf("%s/responses", clientId), h.responseHandler)
		return nil
	}); err != nil {
		return nil, err
	}

	h.responseTopic = fmt.Sprint(responseTopic, clientId)

	_, err := cm.Subscribe(ctx, &paho.Subscribe{
		Subscriptions: map[string]paho.SubscribeOptions{
			fmt.Sprintf("%s/responses", clientId): {QoS: 1},
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

func (h *Handler) Request(ctx context.Context, pb *paho.Publish) (*paho.Publish, error) {
	cID := fmt.Sprintf("%d", time.Now().UnixNano())
	rChan := make(chan *paho.Publish)

	h.addCorrelID(cID, rChan)

	if pb.Properties == nil {
		pb.Properties = &paho.PublishProperties{}
	}

	pb.Properties.CorrelationData = []byte(cID)
	pb.Properties.ResponseTopic = h.responseTopic
	pb.Retain = false

	_, err := h.cm.Publish(ctx, pb)
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
