package packets

import (
	"bytes"
	"io"
	"net"
)

// Pingresp is the Variable Header definition for a Pingresp control packet
type Pingresp struct {
}

func (p *Pingresp) String() string {
	return "PINGRESP"
}

// Unpack is the implementation of the interface required function for a packet
func (p *Pingresp) Unpack(r *bytes.Buffer) error {
	return nil
}

// Buffers is the implementation of the interface required function for a packet
func (p *Pingresp) Buffers() net.Buffers {
	return nil
}

// WriteTo is the implementation of the interface required function for a packet
func (p *Pingresp) WriteTo(w io.Writer) (int64, error) {
	return p.ToControlPacket().WriteTo(w)
}

// ToControlPacket is the implementation of the interface required function for a packet
func (p *Pingresp) ToControlPacket() *ControlPacket {
	return &ControlPacket{FixedHeader: FixedHeader{Type: PINGRESP}, Content: p}
}
