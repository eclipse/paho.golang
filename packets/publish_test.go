package packets

import (
	"bytes"
	"testing"
)

// TestPublishPackUnpack packs and unpacks PUBLISH messages at each QOS level and confirms no loss of data
// Trigger was issue #149 which highlighted a difference between the way PUBLISH packets were encoded depending
// upon whether *ControlPacket.WriteTo or *Publish.WriteTo was called (these should have identical results)
func TestPublishPackUnpack(t *testing.T) {
	const controlPacketWriteTo = "ControlPacket.WriteTo"
	const publishWriteTo = "Publish.WriteTo"

	for _, wt := range []string{controlPacketWriteTo, publishWriteTo} {
		for qos := byte(0); qos < 3; qos++ {
			srcCp := NewControlPacket(PUBLISH)
			srcP := srcCp.Content.(*Publish)
			srcP.PacketID = 1
			srcP.QoS = qos
			srcP.Topic = "Test"
			srcP.Payload = []byte("Test")

			var b bytes.Buffer
			var err error

			switch wt {
			case controlPacketWriteTo:
				_, err = srcCp.WriteTo(&b)
			case publishWriteTo:
				_, err = srcP.WriteTo(&b)
			default:
				panic("bug in test")
			}
			if err != nil {
				t.Errorf("%s failed to Write PUBLISH: %s", wt, err)
			}

			dstCp, err := ReadPacket(bytes.NewReader(b.Bytes()))
			if err != nil {
				t.Errorf("%s failed to Read PUBLISH: %s", wt, err)
			}
			dstP, ok := dstCp.Content.(*Publish)
			if !ok {
				t.Fatalf("%s readPacket did not return expected type (got %T)", wt, dstCp.Content)
			}

			if (qos == 0 && dstP.PacketID != 0) || (qos == 1 && dstP.PacketID != 1) {
				t.Errorf("%s QOS %d unexpected Packet ID: %d", wt, qos, dstP.PacketID)
			}
			if dstP.QoS != qos {
				t.Errorf("%s QOS %d unexpected QOS in decoded packet: %d", wt, qos, dstP.QoS)
			}
			if dstP.Topic != "Test" {
				t.Errorf("%s QOS %d unexpected topic:%s", wt, qos, dstP.Topic)
			}
			if bytes.Compare(dstP.Payload, []byte("Test")) != 0 {
				t.Errorf("%s QOS %d unexpected body: %s", wt, qos, dstP.Payload)
			}
		}
	}
}
