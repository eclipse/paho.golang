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

func TestMidNoExhaustion(t *testing.T) {
	ts := newTestServer()
	ts.SetResponse(packets.PUBACK, &packets.Puback{
		ReasonCode: packets.PubackSuccess,
		Properties: &packets.Properties{},
	})
	go ts.Run()
	defer ts.Stop()

	c := NewClient(ClientConfig{
		Conn: ts.ClientConn(),
	})
	require.NotNil(t, c)

	c.serverInflight = semaphore.NewWeighted(10)
	c.clientInflight = semaphore.NewWeighted(10)
	c.stop = make(chan struct{})
	c.publishPackets = make(chan *packets.Publish)
	go c.Incoming()
	go c.PingHandler.Start(c.Conn, 30*time.Second)

	for i := 0; i < 70000; i++ {
		p := &Publish{
			Topic:   "test/1",
			QoS:     1,
			Payload: []byte("test payload"),
		}

		pa, err := c.Publish(context.Background(), p)
		require.Nil(t, err)
		assert.Equal(t, uint8(0), pa.ReasonCode)
	}

	time.Sleep(10 * time.Millisecond)
}

func TestMidExhaustion(t *testing.T) {
	c := NewClient(ClientConfig{
		Conn: nil,
	})
	require.NotNil(t, c)

	c.serverInflight = semaphore.NewWeighted(10)
	c.clientInflight = semaphore.NewWeighted(10)
	c.stop = make(chan struct{})
	c.publishPackets = make(chan *packets.Publish)
	c.SetDebugLogger(log.New(os.Stderr, "PUBLISHQOS1: ", log.LstdFlags))

	cp := &CPContext{}
	for i := range c.MIDs.(*MIDs).index {
		c.MIDs.(*MIDs).index[i] = cp
	}

	p := &Publish{
		Topic:   "test/1",
		QoS:     1,
		Payload: []byte("test payload"),
	}

	pa, err := c.Publish(context.Background(), p)
	assert.Nil(t, pa)
	assert.ErrorIs(t, err, ErrorMidsExhausted)
}

func BenchmarkRequestMID(b *testing.B) {
	m := &MIDs{index: make([]*CPContext, 65535)}
	cp := &CPContext{}
	for i := 0; i < b.N; i++ {
		v, _ := m.Request(cp)
		m.Free(v)
	}
}
