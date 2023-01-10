package rpc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/eclipse/paho.golang/paho"
)

// Handler is the struct providing a request/response functionality for the paho
// MQTT v5 client
type Handler struct {
	sync.Mutex
	c              *paho.Client
	correlData     map[string]chan *paho.Publish
	requestTimeout *time.Duration
}

// HandlerInput is the handler inputs
type HandlerInput struct {
	Client         *paho.Client   // Client is the paho client
	RequestTimeout *time.Duration // RequestTimeout - if the value sets, it will be used as the request's timeout config
}

func NewHandler(ctx context.Context, in HandlerInput) (*Handler, error) {
	h := &Handler{
		c:              in.Client,
		correlData:     make(map[string]chan *paho.Publish),
		requestTimeout: in.RequestTimeout,
	}

	h.c.Router.RegisterHandler(fmt.Sprintf("%s/responses", h.c.ClientID), h.responseHandler)

	_, err := h.c.Subscribe(ctx, &paho.Subscribe{
		Subscriptions: map[string]paho.SubscribeOptions{
			fmt.Sprintf("%s/responses", h.c.ClientID): {QoS: 1},
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

func (h *Handler) Request(ctx context.Context, pb *paho.Publish) (resp *paho.Publish, err error) {
	cID := fmt.Sprintf("%d", time.Now().UnixNano())
	rChan := make(chan *paho.Publish)

	h.addCorrelID(cID, rChan)

	if pb.Properties == nil {
		pb.Properties = &paho.PublishProperties{}
	}

	pb.Properties.CorrelationData = []byte(cID)
	pb.Properties.ResponseTopic = fmt.Sprintf("%s/responses", h.c.ClientID)
	pb.Retain = false

	_, err = h.c.Publish(ctx, pb)
	if err != nil {
		return nil, err
	}

	requestCtx := ctx
	if h.requestTimeout != nil {
		var cancel context.CancelFunc
		requestCtx, cancel = context.WithTimeout(ctx, *h.requestTimeout)
		defer cancel()
	}

	select {
	case <-requestCtx.Done():
		return nil, requestCtx.Err()
	case resp = <-rChan:
		return resp, nil
	}
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
