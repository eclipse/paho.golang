package state

import (
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho/session"
	"github.com/stretchr/testify/assert"
)

// TestPacketIdAllocateAndFreeAll checks that we can allocate all packet identifiers and that, when freed, a message is always
// sent to the response channel
func TestPacketIdAllocateAndFreeAll(t *testing.T) {
	ss := NewInMemory()
	ss.clientPackets = make(map[uint16]clientGenerated)
	ss.inflight = newSendQuota(200) // not testing this but its needed for endClientGenerated to work

	// Use full band
	cpChan := make(chan packets.ControlPacket)
	for i := uint16(1); i != 0; i++ {
		v, _ := ss.allocateNextPacketId(packets.PUBLISH, cpChan)
		assert.Equal(t, i, v)
	}

	// Trying to allocate another ID should fail
	_, err := ss.allocateNextPacketId(packets.PUBLISH, cpChan)
	assert.ErrorIs(t, err, session.ErrPacketIdentifiersExhausted)

	// Free all Mids
	allResponded := make(chan struct{})
	go func() {
		for i := uint16(0); i < midMax; i++ {
			<-cpChan
		}
		close(allResponded)
	}()

	resp := packets.ControlPacket{
		Content: nil,
		FixedHeader: packets.FixedHeader{
			Type:  packets.PUBACK,
			Flags: 0,
		},
	}
	for i := uint16(1); i != 0; i++ {
		assert.NoError(t, ss.endClientGenerated(i, &resp))
	}
	select {
	case <-allResponded:
	case <-time.After(time.Second):
		t.Fatal("did not receive responses")
	}
	select {
	case <-cpChan:
		t.Fatal("unexpected response")
	default:
	}

	// Allocate all Mids again
	for i := uint16(1); i != 0; i++ {
		v, _ := ss.allocateNextPacketId(packets.PUBLISH, cpChan)
		assert.Equal(t, i, v)
	}

	// Closing the store should free all Ids sending a message to the provided channel
	gotCp := make(chan struct{})
	go func() {
		for i := uint16(0); i < midMax; i++ {
			<-cpChan
		}
		close(gotCp)
	}()
	ss.Close()
	select {
	case <-gotCp:
	case <-time.After(time.Second):
		t.Fatal("did not receive responses")
	}
	select {
	case <-cpChan:
		t.Fatal("unexpected response")
	default:
	}
}

// TestPacketIdHoles confirms that random "holes" within the packet ID map will be found and utilised
func TestPacketIdHoles(t *testing.T) {
	ss := NewInMemory()
	ss.clientPackets = make(map[uint16]clientGenerated)
	ss.inflight = newSendQuota(200) // not testing this but its needed for endClientGenerated to work

	// For this test we ignore responses
	cpChan := make(chan packets.ControlPacket)
	defer close(cpChan)
	go func() {
		for range cpChan {
		}
	}()

	// Allocate all Mids
	for i := uint16(1); i != 0; i++ {
		v, _ := ss.allocateNextPacketId(packets.PUBLISH, cpChan)
		assert.Equal(t, i, v)
	}

	resp := packets.ControlPacket{
		Content: nil,
		FixedHeader: packets.FixedHeader{
			Type:  packets.PUBACK,
			Flags: 0,
		},
	}

	// Currently MIDs.index is filled in, randomly dig some holes and try to fill in all of them again.
	h := map[uint16]bool{}
	for i := 0; i < 60000; i++ {
		r := uint16(rand.Intn(math.MaxUint16))
		r += 1 // Want 0-65535

		ss.endClientGenerated(r, &resp)
		h[r] = true
	}
	t.Log("Num of holes:", len(h))
	for i := 0; i < len(h); i++ {
		_, err := ss.allocateNextPacketId(packets.PUBLISH, cpChan)
		assert.Nil(t, err)
	}
}

// Expecting TestPAcketIdsFreeZeroID.Free(0) always do nothing (no panic), because 0 identifier is invalid and ignored.
func TestPAcketIdsFreeZeroID(t *testing.T) {
	ss := NewInMemory()

	resp := packets.ControlPacket{
		Content: nil,
		FixedHeader: packets.FixedHeader{
			Type:  packets.PUBACK,
			Flags: 0,
		},
	}
	assert.NotPanics(t, func() { assert.NoError(t, ss.endClientGenerated(0, &resp)) })
}
