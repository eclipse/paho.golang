package paho

import (
	"context"
	"errors"
	"sync"

	"github.com/netdata/paho.golang/packets"
)

// ErrNoMoreIDs is an error returned when there are no more available packet IDs.
var ErrNoMoreIDs = errors.New("no more packet ids available.")

// MIDService defines the interface for a struct that handles the
// relationship between message ids and CPContexts
// Request() takes a *CPContext and returns a uint16 that is the
// messageid that should be used by the code that called Request()
// Get() takes a uint16 that is a messageid and returns the matching
// *CPContext that the MIDService has associated with that messageid
// Free() takes a uint16 that is a messageid and instructs the MIDService
// to mark that messageid as available for reuse
// Clear() resets the internal state of the MIDService
type MIDService interface {
	Request(*CPContext) (uint16, error)
	Get(uint16) *CPContext
	Free(uint16)
	Clear()
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
// It uses a map of uint16 to *CPContext to track responses
// to messages with a messageid
type MIDs struct {
	sync.Mutex
	index  map[uint16]*CPContext
	lastID uint16
}

// Request is the library provided MIDService's implementation of
// the required interface function()
func (m *MIDs) Request(c *CPContext) (uint16, error) {
	m.Lock()
	defer m.Unlock()

	for i, n := m.lastID, 0; n < 65535; i, n = i+1, n+1 {
		if i == 0 {
			i = 1
		}

		if _, ok := m.index[i]; !ok {
			m.index[i] = c
			m.lastID = i
			return i, nil
		}
	}

	return 0, ErrNoMoreIDs
}

// Get is the library provided MIDService's implementation of
// the required interface function()
func (m *MIDs) Get(i uint16) *CPContext {
	m.Lock()
	defer m.Unlock()
	return m.index[i]
}

// Free is the library provided MIDService's implementation of
// the required interface function()
func (m *MIDs) Free(i uint16) {
	m.Lock()
	delete(m.index, i)
	m.Unlock()
}

// Clear is the library provided MIDService's implementation of
// the required interface function()
func (m *MIDs) Clear() {
	m.index = make(map[uint16]*CPContext)
}
