package paho

import (
	"github.com/netdata/paho.golang/packets"
)

type Trace struct {
	OnSend    func(*SendStartTrace)
	OnRecv    func(*RecvStartTrace)
	OnPublish func(*PublishStartTrace)
}

type PublishStartTrace struct {
	Packet *packets.Publish
	OnDone func(PublishDoneTrace)
}

func (p *PublishStartTrace) done(err error) {
	if p == nil || p.OnDone == nil {
		return
	}
	p.OnDone(PublishDoneTrace{
		Error: err,
	})
}

type PublishDoneTrace struct {
	Error error
}

func (c *Client) tracePublish(p *packets.Publish) *PublishStartTrace {
	fn := c.Trace.OnPublish
	if fn == nil {
		return nil
	}
	t := PublishStartTrace{
		Packet: p,
	}
	fn(&t)
	return &t
}

type RecvStartTrace struct {
	OnDone func(RecvDoneTrace)
}

type RecvDoneTrace struct {
	Packet     interface{}
	PacketType packets.PacketType
	Error      error
}

func (c *Client) traceRecv() *RecvStartTrace {
	fn := c.Trace.OnRecv
	if fn == nil {
		return nil
	}
	var t RecvStartTrace
	fn(&t)
	return &t
}

func (t *RecvStartTrace) done(x interface{}, err error) {
	if t == nil || t.OnDone == nil {
		return
	}
	t.OnDone(RecvDoneTrace{
		Packet:     x,
		PacketType: matchPacketType(x),
		Error:      err,
	})
}

type SendStartTrace struct {
	Packet     interface{}
	PacketType packets.PacketType

	OnDone func(SendDoneTrace)
}

type SendDoneTrace struct {
	Error error
}

func (c *Client) traceSend(x interface{}) *SendStartTrace {
	fn := c.Trace.OnSend
	if fn == nil {
		return nil
	}
	t := SendStartTrace{
		Packet:     x,
		PacketType: matchPacketType(x),
	}
	fn(&t)
	return &t
}

func (t *SendStartTrace) done(err error) {
	if t != nil && t.OnDone != nil {
		t.OnDone(SendDoneTrace{
			Error: err,
		})
	}
}

func matchPacketType(x interface{}) packets.PacketType {
	if x == nil {
		return 0
	}
	switch p := x.(type) {
	case *packets.ControlPacket:
		if p == nil {
			return 0
		}
		return p.FixedHeader.Type

	case *packets.Connect:
		return packets.CONNECT
	case *packets.Connack:
		return packets.CONNACK
	case *packets.Publish:
		return packets.PUBLISH
	case *packets.Puback:
		return packets.PUBACK
	case *packets.Pubrec:
		return packets.PUBREC
	case *packets.Pubrel:
		return packets.PUBREL
	case *packets.Pubcomp:
		return packets.PUBCOMP
	case *packets.Subscribe:
		return packets.SUBSCRIBE
	case *packets.Suback:
		return packets.SUBACK
	case *packets.Unsubscribe:
		return packets.UNSUBSCRIBE
	case *packets.Unsuback:
		return packets.UNSUBACK
	case *packets.Pingreq:
		return packets.PINGREQ
	case *packets.Pingresp:
		return packets.PINGRESP
	case *packets.Disconnect:
		return packets.DISCONNECT
	case *packets.Auth:
		return packets.AUTH

	default:
		return 0
	}
}
