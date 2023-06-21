package paho

import (
	"github.com/eclipse/paho.golang/packets"
)

type OrphanedOps interface {
	AddOp(<-chan packets.ControlPacket)
	Stop()
}

type StandardOrphanedOps struct {
	stop                chan struct{}
	publishCallback     func(*PublishResponse)
	subscribeCallback   func(*Suback)
	unsubscribeCallback func(*Unsuback)
}

type OrphanedOperationPoolCallbacks struct {
	PublishCallback     func(*PublishResponse)
	SubscribeCallback   func(*Suback)
	UnsubscribeCallback func(*Unsuback)
}

func NewStandardOrphanedOps(callbacks OrphanedOperationPoolCallbacks) *StandardOrphanedOps {
	var (
		publishCallback     func(*PublishResponse)
		subscribeCallback   func(*Suback)
		unsubscribeCallback func(*Unsuback)
	)
	if callbacks.PublishCallback == nil {
		publishCallback = func(*PublishResponse) {}
	} else {
		publishCallback = callbacks.PublishCallback
	}
	if callbacks.SubscribeCallback == nil {
		subscribeCallback = func(*Suback) {}
	} else {
		subscribeCallback = callbacks.SubscribeCallback
	}
	if callbacks.UnsubscribeCallback == nil {
		unsubscribeCallback = func(*Unsuback) {}
	} else {
		unsubscribeCallback = callbacks.UnsubscribeCallback
	}

	return &StandardOrphanedOps{
		stop:                make(chan struct{}),
		publishCallback:     publishCallback,
		subscribeCallback:   subscribeCallback,
		unsubscribeCallback: unsubscribeCallback,
	}
}

func (p *StandardOrphanedOps) AddOp(opResp <-chan packets.ControlPacket) {
	go func() {
		var ok bool
		var opResult packets.ControlPacket
		select {
		case <-p.stop:
			return
		case opResult, ok = <-opResp:
			if !ok {
				return
			}
		}
		switch opResult.Type {

		case packets.PUBACK:
			p.publishCallback(PublishResponseFromPuback(opResult.Content.(*packets.Puback)))
		case packets.PUBREC:
			p.publishCallback(PublishResponseFromPubrec(opResult.Content.(*packets.Pubrec)))
		case packets.PUBCOMP:
			p.publishCallback(PublishResponseFromPubcomp(opResult.Content.(*packets.Pubcomp)))
		case packets.SUBACK:
			p.subscribeCallback(SubackFromPacketSuback(opResult.Content.(*packets.Suback)))
		case packets.UNSUBACK:
			p.unsubscribeCallback(UnsubackFromPacketUnsuback(opResult.Content.(*packets.Unsuback)))
		}
	}()
}

func (p *StandardOrphanedOps) Stop() {
	close(p.stop)
}
