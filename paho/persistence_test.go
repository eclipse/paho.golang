package paho

import (
	"sync"

	"github.com/eclipse/paho.golang/packets"
)

type testPersistence struct {
	putCount    int
	getCount    int
	deleteCount int
	sync.RWMutex
	packets map[uint16]packets.Packet
}

func newTestPersistence() *testPersistence {
	return &testPersistence{
		packets: make(map[uint16]packets.Packet),
	}
}

func (t *testPersistence) Open() error {
	t.Lock()
	if t.packets == nil {
		t.packets = make(map[uint16]packets.Packet)
	}
	t.Unlock()

	return nil
}

func (t *testPersistence) Put(id uint16, cp packets.Packet) error {
	t.Lock()
	t.putCount++
	t.packets[id] = cp
	t.Unlock()

	return nil
}

func (t *testPersistence) Get(id uint16) packets.Packet {
	t.RLock()
	defer t.RUnlock()
	t.getCount++
	return t.packets[id]
}

func (t *testPersistence) All() []packets.Packet {
	t.Lock()
	defer t.RUnlock()
	ret := make([]packets.Packet, len(t.packets))

	for _, cp := range t.packets {
		ret = append(ret, cp)
	}

	return ret
}

func (t *testPersistence) Delete(id uint16) {
	t.Lock()
	delete(t.packets, id)
	t.deleteCount++
	t.Unlock()
}

func (t *testPersistence) Close() {
	t.Lock()
	t.packets = nil
	t.Unlock()
}

func (t *testPersistence) Reset() {
	t.Lock()
	t.packets = make(map[uint16]packets.Packet)
	t.Unlock()
}

func (t *testPersistence) Len() int {
	t.Lock()
	defer t.Unlock()
	return len(t.packets)
}
