package paho

import (
	"context"

	"github.com/netdata/paho.golang/packets"
)

type Trace struct {
	OnSend    func(context.Context, *SendStartTrace)
	OnRecv    func(context.Context, *RecvStartTrace)
	OnPublish func(context.Context, *PublishStartTrace)
}

type PublishStartTrace struct {
	Packet *packets.Publish
	OnDone func(context.Context, PublishDoneTrace)
}

func (p *PublishStartTrace) done(ctx context.Context, err error) {
	if p == nil || p.OnDone == nil {
		return
	}
	p.OnDone(ctx, PublishDoneTrace{
		Error: err,
	})
}

type PublishDoneTrace struct {
	Error error
}

func (c *Client) tracePublish(ctx context.Context, p *packets.Publish) *PublishStartTrace {
	fn := c.Trace.OnPublish
	if fn == nil {
		return nil
	}
	t := PublishStartTrace{
		Packet: p,
	}
	fn(ctx, &t)
	return &t
}

type RecvStartTrace struct {
	OnDone func(context.Context, RecvDoneTrace)
}

type RecvDoneTrace struct {
	Packet     interface{}
	PacketType packets.PacketType
	Error      error
}

func (c *Client) traceRecv(ctx context.Context) *RecvStartTrace {
	fn := c.Trace.OnRecv
	if fn == nil {
		return nil
	}
	var t RecvStartTrace
	fn(ctx, &t)
	return &t
}

func (t *RecvStartTrace) done(ctx context.Context, x interface{}, err error) {
	if t == nil || t.OnDone == nil {
		return
	}
	t.OnDone(ctx, RecvDoneTrace{
		Packet:     x,
		PacketType: matchPacketType(x),
		Error:      err,
	})
}

type SendStartTrace struct {
	Packet     interface{}
	PacketType packets.PacketType

	OnDone func(context.Context, SendDoneTrace)
}

type SendDoneTrace struct {
	Error error
}

func (c *Client) traceSend(ctx context.Context, x interface{}) *SendStartTrace {
	fn := c.Trace.OnSend
	if fn == nil {
		return nil
	}
	t := SendStartTrace{
		Packet:     x,
		PacketType: matchPacketType(x),
	}
	fn(ctx, &t)
	return &t
}

func (t *SendStartTrace) done(ctx context.Context, err error) {
	if t != nil && t.OnDone != nil {
		t.OnDone(ctx, SendDoneTrace{
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
