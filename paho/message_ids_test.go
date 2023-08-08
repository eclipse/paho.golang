package paho

import (
	"context"
	"log"
	"os"
	"testing"
	"time"
	"math"
	"math/rand"

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
	go c.incoming()
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

func TestUsingFullBandOfMID(t *testing.T){
	m := &MIDs{index: make([]*CPContext, midMax)}
	cp := &CPContext{}

	// Use full band
	for i := uint16(0); i < midMax; i++ {
		v, _ := m.Request(cp)
		assert.Equal(t, i+1, v)
		m.Free(v)
	}

	// Just in case, try second loop and fill in all MIDs.index
	for i := uint16(0); i < midMax; i++ {
		v, _ := m.Request(cp)
		assert.Equal(t, i+1, v)
	}

	// Current lastMid is expected to be 65535 (midMax). After Free(65535) is called, only MIDs.index[65534] is expected to be blank.
	// So, Request() is expected to find MID 65535 (MIDs.index[65534]) with full band search.
	m.Free(midMax)
	v, _ := m.Request(cp)
	assert.Equal(t, midMax, v)

	// Currently MIDs.index is filled in, and then randomly dig some holes and try to fill in all of them again.
	seed := time.Now().UnixNano()
	rand.Seed(seed)
	h := map[uint16]bool{}
	for i := 0; i < 60000; i++ {
		r := uint16(rand.Intn(math.MaxUint16 + 1))
		if r == 0 {
		  r += 1
		}
		m.Free(r)
		h[r] = true
	}
	t.Log("Num of holes:", len(h))
	for i := 0; i < len(h); i++ {
		_, err := m.Request(cp)
		assert.Nil(t, err)
	}
}

func TestMIDsGetAllIDs(t *testing.T) {
	m := &MIDs{index: make([]*CPContext, midMax)}
	for i := uint16(0); i < midMax; i++ {
		cp := &CPContext{}
		m.index[i] = cp
	}

	for i := uint16(0); i < midMax; i++ {
		assert.NotNil(t, m.Get(i+1))
	}
}

// Expecting MIDs.Get(0) returns always nil, because 0 identifier is invalid and cannot be retained in MIDs.
func TestMIDsGetZeroID(t *testing.T) {
	m := &MIDs{index: make([]*CPContext, midMax)}
	assert.NotPanics(t, func(){ m.Get(0) })
}

func TestMIDsFreeAllIDs(t *testing.T) {
	m := &MIDs{index: make([]*CPContext, midMax)}
	for i := uint16(0); i < midMax; i++ {
		cp := &CPContext{}
		m.index[i] = cp
	}

	for i := uint16(0); i < midMax; i++ {
		m.Free(i+1)
	}

	for i := uint16(0); i < midMax; i++ {
		assert.Nil(t, m.index[i])
	}
}

// Expecting MIDs.Free(0) always do nothing (no panic), because 0 identifier is invalid and ignored.
func TestMIDsFreeZeroID(t *testing.T) {
	m := &MIDs{index: make([]*CPContext, midMax)}
	assert.NotPanics(t, func(){ m.Free(0) })
}

func BenchmarkRequestMID(b *testing.B) {
	m := &MIDs{index: make([]*CPContext, 65535)}
	cp := &CPContext{}
	for i := 0; i < b.N; i++ {
		v, _ := m.Request(cp)
		m.Free(v)
	}
}
