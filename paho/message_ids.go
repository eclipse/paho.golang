package paho

import (
	"context"
	"errors"
	"sync"

	"github.com/eclipse/paho.golang/packets"
)

const (
	midMin uint16 = 1
	midMax uint16 = 65535
)

// ErrorMidsExhausted is returned from Request() when there are no
// free message ids to be used.
var ErrorMidsExhausted = errors.New("all message ids in use")

// MIDService defines the interface for a struct that handles the
// relationship between message ids and CPContexts
// Request() takes a *CPContext and returns a uint16 that is the
// messageid that should be used by the code that called Request()
// Get() takes a uint16 that is a messageid and returns the matching
// *CPContext that the MIDService has associated with that messageid
// Free() takes a uint16 that is a messageid and instructs the MIDService
// to mark that messageid as available for reuse
type MIDService interface {
	Request(*CPContext) (uint16, error)
	Get(uint16) *CPContext
	Free(uint16)
}

// CPContext is the struct that is used to return responses to
// ControlPackets that have them, eg: the suback to a subscribe.
// The response packet is send down the Return channel and the
// Context is used to track timeouts.
type CPContext struct {
	Context context.Context
	Return  chan packets.ControlPacket
}

// MIDs is the default MIDService provided by this library.
// It uses a slice of *CPContext to track responses
// to messages with a messageid tracking the last used message id
type MIDs struct {
	sync.RWMutex
	lastMid uint16
	index   []*CPContext // index of slice is (messageid - 1)
}

// NewMIDs returns a new MIDs instance with message IDs claimed using
// the information in the supplied map. nil may be passed if no message IDs
// need to be claimed.
func NewMIDs(inUse map[uint16]*CPContext) *MIDs {
	m := &MIDs{index: make([]*CPContext, midMax)}
	for mid, c := range inUse {
		m.index[mid-1] = c
		if mid > m.lastMid {
			m.lastMid = mid
		}
	}
	return m
}

// Request is the library provided MIDService's implementation of
// the required interface function()
func (m *MIDs) Request(c *CPContext) (uint16, error) {
	m.Lock()
	defer m.Unlock()

	// Scan from lastMid to end of range.
	for i := m.lastMid; i < midMax; i++ {
		if m.index[i] != nil {
			continue
		}
		m.index[i] = c
		m.lastMid = i + 1
		return i + 1, nil
	}
	// Scan from start of range to lastMid
	for i := uint16(0); i < m.lastMid; i++ {
		if m.index[i] != nil {
			continue
		}
		m.index[i] = c
		m.lastMid = i + 1
		return i + 1, nil
	}

	return 0, ErrorMidsExhausted
}

// Get is the library provided MIDService's implementation of
// the required interface function()
func (m *MIDs) Get(i uint16) *CPContext {
	m.RLock()
	defer m.RUnlock()
	return m.index[i-1]
}

// Free is the library provided MIDService's implementation of
// the required interface function()
func (m *MIDs) Free(i uint16) {
	m.Lock()
	m.index[i-1] = nil
	m.Unlock()
}
