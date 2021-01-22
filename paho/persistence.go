package paho

import (
	"sync"

	"github.com/eclipse/paho.golang/packets"
)

// Persistence is an interface of the functions for a struct
// that is used to persist Packets.
// Open() is an initialiser to prepare the Persistence for use
// Put() takes a uint16 which is a messageid and a Packet
// to persist against that messageid
// Get() takes a uint16 which is a messageid and returns the
// persisted Packet from the Persistence for that messageid
// All() returns a slice of all Packets persisted
// Delete() takes a uint16 which is a messageid and deletes the
// associated stored Packet from the Persistence
// Close() closes the Persistence
// Reset() clears the Persistence and prepares it to be reused
type Persistence interface {
	Open() error
	Put(uint16, packets.Packet) error
	Get(uint16) packets.Packet
	All() []packets.Packet
	Delete(uint16)
	Close()
	Reset()
}

// MemoryPersistence is an implementation of a Persistence
// that stores the Packets in memory using a map
type MemoryPersistence struct {
	sync.RWMutex
	packets map[uint16]packets.Packet
}

// Open is the library provided MemoryPersistence's implementation of
// the required interface function()
func (m *MemoryPersistence) Open() error {
	m.Lock()
	if m.packets == nil {
		m.packets = make(map[uint16]packets.Packet)
	}
	m.Unlock()

	return nil
}

// Put is the library provided MemoryPersistence's implementation of
// the required interface function()
func (m *MemoryPersistence) Put(id uint16, cp packets.Packet) error {
	m.Lock()
	m.packets[id] = cp
	m.Unlock()

	return nil
}

// Get is the library provided MemoryPersistence's implementation of
// the required interface function()
func (m *MemoryPersistence) Get(id uint16) packets.Packet {
	m.RLock()
	defer m.RUnlock()
	return m.packets[id]
}

// All is the library provided MemoryPersistence's implementation of
// the required interface function()
func (m *MemoryPersistence) All() []packets.Packet {
	m.Lock()
	defer m.RUnlock()
	ret := make([]packets.Packet, len(m.packets))

	for _, cp := range m.packets {
		ret = append(ret, cp)
	}

	return ret
}

// Delete is the library provided MemoryPersistence's implementation of
// the required interface function()
func (m *MemoryPersistence) Delete(id uint16) {
	m.Lock()
	delete(m.packets, id)
	m.Unlock()
}

// Close is the library provided MemoryPersistence's implementation of
// the required interface function()
func (m *MemoryPersistence) Close() {
	m.Lock()
	m.packets = nil
	m.Unlock()
}

// Reset is the library provided MemoryPersistence's implementation of
// the required interface function()
func (m *MemoryPersistence) Reset() {
	m.Lock()
	m.packets = make(map[uint16]packets.Packet)
	m.Unlock()
}
