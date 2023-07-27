package packets

import (
	"bytes"
	"io"
	"net"
)

// Pingreq is the Variable Header definition for a Pingreq control packet
type Pingreq struct {
}

func (p *Pingreq) String() string {
	return "PINGREQ"
}

// Unpack is the implementation of the interface required function for a packet
func (p *Pingreq) Unpack(r *bytes.Buffer) error {
	return nil
}

// Buffers is the implementation of the interface required function for a packet
func (p *Pingreq) Buffers() net.Buffers {
	return nil
}

// WriteTo is the implementation of the interface required function for a packet
func (p *Pingreq) WriteTo(w io.Writer) (int64, error) {
	return p.ToControlPacket().WriteTo(w)
}

// ToControlPacket is the implementation of the interface required function for a packet
func (p *Pingreq) ToControlPacket() *ControlPacket {
	return &ControlPacket{FixedHeader: FixedHeader{Type: PINGREQ}, Content: p}
}
