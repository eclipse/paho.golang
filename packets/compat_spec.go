package packets

import (
	"bytes"
)

const minVer = 4
const maxVer = 5

var _protocolVersion byte = maxVer

// protocolVersion for swapping or getting MQTT version in packet
func protocolVersion(vers ...byte) byte {
	var ver byte
	if len(vers) > 0 {
		ver = vers[0]
	}
	if minVer <= ver && ver <= maxVer {
		_protocolVersion = ver
	}
	return _protocolVersion
}

func isVer4() bool {
	return _protocolVersion == 4
}

type propPack struct {
	typ byte
}

func (p propPack) Pack(prop *Properties, cp *bytes.Buffer) {
	if isVer4() {
		return
	}
	idvp := prop.Pack(p.typ)
	encodeVBIdirect(len(idvp), cp)
	cp.Write(idvp)
}

func (p propPack) Unpack(r *bytes.Buffer, prop *Properties) error {
	if isVer4() {
		return nil
	}
	err := prop.Unpack(r, p.typ)
	if err != nil {
		return err
	}
	return nil
}

func genPropPack(p byte) propPack {
	return propPack{typ: p}
}

func connackReasonFromV311(code byte) byte {
	switch code {
	case 0x00:
		return ConnackSuccess
	case 0x01:
		return ConnackUnsupportedProtocolVersion
	case 0x02:
		return ConnackInvalidClientID
	case 0x03:
		return ConnackServerUnavailable
	case 0x04:
		return ConnackBadUsernameOrPassword
	case 0x05:
		return ConnackNotAuthorized
	default:
		return ConnackUnspecifiedError
	}
}

func connackReasonToV311(code byte) byte {
	switch code {
	case ConnackSuccess:
		return 0x00
	case ConnackUnsupportedProtocolVersion:
		return 0x01
	case ConnackInvalidClientID:
		return 0x02
	case ConnackServerUnavailable:
		return 0x03
	case ConnackBadUsernameOrPassword:
		return 0x04
	case ConnackNotAuthorized:
		return 0x05
	default:
		return 0x03
	}
}

func subackReasonsFromV311(codes []byte) []byte {
	for i := 0; i < len(codes); i++ {
		codes[i] = subackReasonV311{codes[i]}.Decode()
	}
	return codes
}

func subackReasonsToV311(codes []byte) []byte {
	for i := 0; i < len(codes); i++ {
		codes[i] = subackReasonV311{codes[i]}.Encode()
	}
	return codes
}

type subackReasonV311 struct {
	code byte
}

func (r subackReasonV311) Decode() byte {
	switch r.code {
	case 0x00:
		return SubackGrantedQoS0
	case 0x01:
		return SubackGrantedQoS1
	case 0x02:
		return SubackGrantedQoS2
	case 0x80:
		fallthrough
	default:
		return SubackUnspecifiederror
	}
}

func (r subackReasonV311) Encode() byte {
	switch r.code {
	case SubackGrantedQoS0:
		return 0x00
	case SubackGrantedQoS1:
		return 0x01
	case SubackGrantedQoS2:
		return 0x02
	case SubackUnspecifiederror:
		fallthrough
	default:
		return 0x80
	}
}
