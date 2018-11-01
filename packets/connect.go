package packets

import (
	"bytes"
	"io"
	"net"
)

// Connect is the Variable Header definition for a connect control packet
type Connect struct {
	PasswordFlag    bool
	UsernameFlag    bool
	ProtocolName    string
	ProtocolVersion byte
	WillTopic       string
	WillRetain      bool
	WillQOS         byte
	WillFlag        bool
	WillMessage     []byte
	CleanStart      bool
	Username        string
	Password        []byte
	KeepAlive       uint16
	ClientID        string
	Properties      *Properties
	WillProperties  *Properties
}

// PackFlags takes the Connect flags and packs them into the single byte
// representation used on the wire by MQTT
func (c *Connect) PackFlags() (f byte) {
	if c.UsernameFlag {
		f |= 0x01 << 7
	}
	if c.PasswordFlag {
		f |= 0x01 << 6
	}
	if c.WillFlag {
		f |= 0x01 << 2
		f |= c.WillQOS << 3
		if c.WillRetain {
			f |= 0x01 << 5
		}
	}
	if c.CleanStart {
		f |= 0x01 << 1
	}
	return
}

// UnpackFlags takes the wire byte representing the connect options flags
// and fills out the appropriate variables in the struct
func (c *Connect) UnpackFlags(b byte) {
	c.CleanStart = 1&(b>>1) > 0
	c.WillFlag = 1&(b>>2) > 0
	c.WillQOS = 3 & (b >> 3)
	c.WillRetain = 1&(b>>5) > 0
	c.PasswordFlag = 1&(b>>6) > 0
	c.UsernameFlag = 1&(b>>7) > 0
}

//Unpack is the implementation of the interface required function for a packet
func (c *Connect) Unpack(r *bytes.Buffer) error {
	var err error

	if c.ProtocolName, err = readString(r); err != nil {
		return err
	}

	if c.ProtocolVersion, err = r.ReadByte(); err != nil {
		return err
	}

	flags, err := r.ReadByte()
	if err != nil {
		return err
	}
	c.UnpackFlags(flags)

	if c.KeepAlive, err = readUint16(r); err != nil {
		return err
	}

	err = c.Properties.Unpack(r, CONNECT)
	if err != nil {
		return err
	}

	c.ClientID, err = readString(r)
	if err != nil {
		return err
	}

	if c.WillFlag {
		err = c.WillProperties.Unpack(r, CONNECT)
		if err != nil {
			return err
		}
		c.WillTopic, err = readString(r)
		if err != nil {
			return err
		}
		c.WillMessage, err = readBinary(r)
		if err != nil {
			return err
		}
	}

	if c.UsernameFlag {
		c.Username, err = readString(r)
		if err != nil {
			return err
		}
	}

	if c.PasswordFlag {
		c.Password, err = readBinary(r)
		if err != nil {
			return err
		}
	}

	return nil
}

// Buffers is the implementation of the interface required function for a packet
func (c *Connect) Buffers() net.Buffers {
	var header bytes.Buffer
	var body bytes.Buffer
	writeString(c.ProtocolName, &header)
	header.WriteByte(c.ProtocolVersion)
	header.WriteByte(c.PackFlags())
	writeUint16(c.KeepAlive, &header)
	idvp := c.Properties.Pack(CONNECT)
	propLen := encodeVBI(len(idvp))

	writeString(c.ClientID, &body)
	if c.WillFlag {
		willIdvp := c.WillProperties.Pack(CONNECT)
		writeBinary(encodeVBI(len(willIdvp)), &body)
		writeBinary(willIdvp, &body)
		writeString(c.WillTopic, &body)
		writeBinary(c.WillMessage, &body)
	}
	if c.UsernameFlag {
		writeString(c.Username, &body)
	}
	if c.PasswordFlag {
		writeBinary(c.Password, &body)
	}

	return net.Buffers{header.Bytes(), propLen, idvp, body.Bytes()}
}

// WriteTo is the implementation of the interface required function for a packet
func (c *Connect) WriteTo(w io.Writer) (int64, error) {
	cp := &ControlPacket{FixedHeader: FixedHeader{Type: CONNECT}}
	cp.Content = c

	return cp.WriteTo(w)
}
