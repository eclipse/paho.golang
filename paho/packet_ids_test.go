package paho

import (
	"context"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/internal/basictestserver"
	"github.com/eclipse/paho.golang/packets"
	paholog "github.com/eclipse/paho.golang/paho/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPackedIdNoExhaustion tests interactions between Publish and the session ensuring that IDs are
// released and reused
func TestPackedIdNoExhaustion(t *testing.T) {
	ts := basictestserver.New(paholog.NewTestLogger(t, "TestServer:"))
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

	c.stop = make(chan struct{})
	c.publishPackets = make(chan *packets.Publish)
	go c.incoming()
	go c.config.PingHandler.Start(c.config.Conn, 30*time.Second)
	c.config.Session.ConAckReceived(c.config.Conn, &packets.Connect{}, &packets.Connack{})

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

// Note: We no longer test for Packet Id Exhaustion because the way the CONNACK Receive Maximum now works makes
// this impossible (the semaphore will lock on the 65536th request and only unlock when a response is received
// which would also mean an ID is available).
