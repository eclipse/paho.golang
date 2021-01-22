package paho

import "github.com/eclipse/paho.golang/packets"

type noopPersistence struct{}

func (n *noopPersistence) Open() error {
	return nil
}

func (n *noopPersistence) Put(id uint16, cp packets.Packet) error {
	return nil
}

func (n *noopPersistence) Get(id uint16) packets.Packet {
	return nil
}

func (n *noopPersistence) All() []packets.Packet {
	return nil
}

func (n *noopPersistence) Delete(id uint16) {}

func (n *noopPersistence) Close() {}

func (n *noopPersistence) Reset() {}
