package paho

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/packets"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

func TestNewClient(t *testing.T) {
	c := NewClient()

	require.NotNil(t, c)
	require.NotNil(t, c.stop)
	require.NotNil(t, c.Persistence)
	require.NotNil(t, c.MIDs)
	require.NotNil(t, c.Router)
	require.NotNil(t, c.PingHandler)

	assert.Equal(t, uint16(65535), c.serverProps.ReceiveMaximum)
	assert.Equal(t, uint8(2), c.serverProps.MaximumQoS)
	assert.Equal(t, uint32(0), c.serverProps.MaximumPacketSize)
	assert.Equal(t, uint16(0), c.serverProps.TopicAliasMaximum)
	assert.True(t, c.serverProps.RetainAvailable)
	assert.True(t, c.serverProps.WildcardSubAvailable)
	assert.True(t, c.serverProps.SubIDAvailable)
	assert.True(t, c.serverProps.SharedSubAvailable)

	assert.Equal(t, uint16(65535), c.clientProps.ReceiveMaximum)
	assert.Equal(t, uint8(2), c.clientProps.MaximumQoS)
	assert.Equal(t, uint32(0), c.clientProps.MaximumPacketSize)
	assert.Equal(t, uint16(0), c.clientProps.TopicAliasMaximum)

	assert.Equal(t, 10*time.Second, c.PacketTimeout)
}

func TestClientConnect(t *testing.T) {
	SetDebugLogger(log.New(os.Stderr, "CONNECT: ", log.LstdFlags))
	ts := newTestServer()
	ts.SetResponse(packets.CONNACK, &packets.Connack{
		ReasonCode:     0,
		SessionPresent: false,
		Properties: &packets.Properties{
			MaximumPacketSize: Uint32(12345),
			MaximumQOS:        Byte(1),
			ReceiveMaximum:    Uint16(12345),
			TopicAliasMaximum: Uint16(200),
		},
	})
	go ts.Run()
	defer ts.Stop()

	c := NewClient()
	require.NotNil(t, c)

	c.Conn = ts.ClientConn()

	cp := &Connect{
		KeepAlive:  30,
		ClientID:   "testClient",
		CleanStart: true,
		Properties: &ConnectProperties{
			ReceiveMaximum: Uint16(200),
		},
		WillMessage: &WillMessage{
			Topic:   "will/topic",
			Payload: []byte("am gone"),
		},
		WillProperties: &WillProperties{
			WillDelayInterval: Uint32(200),
		},
	}

	ca, err := c.Connect(context.Background(), cp)
	require.Nil(t, err)
	assert.Equal(t, uint8(0), ca.ReasonCode)

	time.Sleep(10 * time.Millisecond)
}

func TestClientSubscribe(t *testing.T) {
	SetDebugLogger(log.New(os.Stderr, "SUBSCRIBE: ", log.LstdFlags))
	ts := newTestServer()
	ts.SetResponse(packets.SUBACK, &packets.Suback{
		Reasons:    []byte{1, 2, 0},
		Properties: &packets.Properties{},
	})
	go ts.Run()
	defer ts.Stop()

	c := NewClient()
	require.NotNil(t, c)

	c.Conn = ts.ClientConn()
	go c.Incoming()
	go c.PingHandler.Start(c.Conn, 30*time.Second)

	s := &Subscribe{
		Subscriptions: map[string]SubscribeOptions{
			"test/1": SubscribeOptions{QoS: 1},
			"test/2": SubscribeOptions{QoS: 2},
			"test/3": SubscribeOptions{QoS: 0},
		},
	}

	sa, err := c.Subscribe(context.Background(), s)
	require.Nil(t, err)
	assert.Equal(t, []byte{1, 2, 0}, sa.Reasons)

	time.Sleep(10 * time.Millisecond)
}

func TestClientUnsubscribe(t *testing.T) {
	SetDebugLogger(log.New(os.Stderr, "UNSUBSCRIBE: ", log.LstdFlags))
	ts := newTestServer()
	ts.SetResponse(packets.UNSUBACK, &packets.Unsuback{
		Reasons:    []byte{0, 17},
		Properties: &packets.Properties{},
	})
	go ts.Run()
	defer ts.Stop()

	c := NewClient()
	require.NotNil(t, c)

	c.Conn = ts.ClientConn()
	go c.Incoming()
	go c.PingHandler.Start(c.Conn, 30*time.Second)

	u := &Unsubscribe{
		Topics: []string{
			"test/1",
			"test/2",
		},
	}

	ua, err := c.Unsubscribe(context.Background(), u)
	require.Nil(t, err)
	assert.Equal(t, []byte{0, 17}, ua.Reasons)

	time.Sleep(10 * time.Millisecond)
}

func TestClientPublishQoS1(t *testing.T) {
	SetDebugLogger(log.New(os.Stderr, "PUBLISHQOS1: ", log.LstdFlags))
	ts := newTestServer()
	ts.SetResponse(packets.PUBACK, &packets.Puback{
		ReasonCode: packets.PubackSuccess,
		Properties: &packets.Properties{},
	})
	go ts.Run()
	defer ts.Stop()

	c := NewClient()
	require.NotNil(t, c)

	c.Conn = ts.ClientConn()
	c.serverInflight = semaphore.NewWeighted(10000)
	c.clientInflight = semaphore.NewWeighted(10000)
	go c.Incoming()
	go c.PingHandler.Start(c.Conn, 30*time.Second)

	p := &Publish{
		Topic:   "test/2",
		QoS:     1,
		Payload: []byte("test payload"),
	}

	pa, err := c.Publish(context.Background(), p)
	require.Nil(t, err)
	assert.Equal(t, uint8(0), pa.ReasonCode)

	time.Sleep(10 * time.Millisecond)
}

func TestClientPublishQoS2(t *testing.T) {
	SetDebugLogger(log.New(os.Stderr, "PUBLISHQOS2: ", log.LstdFlags))
	ts := newTestServer()
	ts.SetResponse(packets.PUBREC, &packets.Pubrec{
		ReasonCode: packets.PubrecSuccess,
		Properties: &packets.Properties{},
	})
	ts.SetResponse(packets.PUBCOMP, &packets.Pubcomp{
		ReasonCode: packets.PubcompSuccess,
		Properties: &packets.Properties{},
	})
	go ts.Run()
	defer ts.Stop()

	c := NewClient()
	require.NotNil(t, c)

	c.Conn = ts.ClientConn()
	c.serverInflight = semaphore.NewWeighted(10000)
	c.clientInflight = semaphore.NewWeighted(10000)
	go c.Incoming()
	go c.PingHandler.Start(c.Conn, 30*time.Second)

	p := &Publish{
		Topic:   "test/2",
		QoS:     2,
		Payload: []byte("test payload"),
	}

	pr, err := c.Publish(context.Background(), p)
	require.Nil(t, err)
	assert.Equal(t, uint8(0), pr.ReasonCode)

	time.Sleep(10 * time.Millisecond)
}

func TestClientReceiveQoS1(t *testing.T) {
	rChan := make(chan struct{})
	SetDebugLogger(log.New(os.Stderr, "RECEIVEQOS1: ", log.LstdFlags))
	ts := newTestServer()
	go ts.Run()
	defer ts.Stop()

	c := NewClient()
	require.NotNil(t, c)

	c.Router = NewSingleHandlerRouter(func(p *Publish) {
		assert.Equal(t, "test/1", p.Topic)
		assert.Equal(t, "test payload", string(p.Payload))
		assert.Equal(t, byte(1), p.QoS)
		close(rChan)
	})

	c.Conn = ts.ClientConn()
	c.serverInflight = semaphore.NewWeighted(10000)
	c.clientInflight = semaphore.NewWeighted(10000)
	go c.Incoming()
	go c.PingHandler.Start(c.Conn, 30*time.Second)

	ts.SendPacket(&packets.Publish{
		Topic:   "test/1",
		QoS:     1,
		Payload: []byte("test payload"),
	})

	<-rChan
}

func TestClientReceiveQoS2(t *testing.T) {
	rChan := make(chan struct{})
	SetDebugLogger(log.New(os.Stderr, "RECEIVEQOS2: ", log.LstdFlags))
	ts := newTestServer()
	go ts.Run()
	defer ts.Stop()

	c := NewClient()
	require.NotNil(t, c)

	c.Router = NewSingleHandlerRouter(func(p *Publish) {
		assert.Equal(t, "test/2", p.Topic)
		assert.Equal(t, "test payload", string(p.Payload))
		assert.Equal(t, byte(2), p.QoS)
		close(rChan)
	})

	c.Conn = ts.ClientConn()
	c.serverInflight = semaphore.NewWeighted(10000)
	c.clientInflight = semaphore.NewWeighted(10000)
	go c.Incoming()
	go c.PingHandler.Start(c.Conn, 30*time.Second)

	ts.SendPacket(&packets.Publish{
		Topic:   "test/2",
		QoS:     2,
		Payload: []byte("test payload"),
	})

	<-rChan
}
