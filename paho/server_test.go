package paho

import (
	"log"
	"net"

	"github.com/eclipse/paho.golang/packets"
)

type testServer struct {
	conn       net.Conn
	clientConn net.Conn
	stop       chan struct{}
	responses  map[packets.PacketType]packets.Packet
}

func newTestServer() *testServer {
	t := &testServer{
		stop:      make(chan struct{}),
		responses: make(map[packets.PacketType]packets.Packet),
	}
	t.conn, t.clientConn = net.Pipe()

	return t
}

func (t *testServer) ClientConn() net.Conn {
	return t.clientConn
}

func (t *testServer) SetResponse(pt packets.PacketType, p packets.Packet) {
	t.responses[pt] = p
}

func (t *testServer) Stop() {
	t.conn.Close()
	close(t.stop)
}

func (t *testServer) Run() {
	for {
		select {
		case <-t.stop:
			return
		default:
			recv, err := packets.ReadPacket(t.conn)
			if err != nil {
				log.Println("error in test server reading packet", err)
				return
			}
			log.Println("test server received a control packet:", recv.Type)
			switch recv.Type {
			case packets.CONNECT:
				log.Println("received connect", recv.Content.(*packets.Connect))
				if p, ok := t.responses[packets.CONNACK]; ok {
					if _, err := p.WriteTo(t.conn); err != nil {
						log.Println(err)
					}
				}
			case packets.SUBSCRIBE:
				log.Println("received subscribe", recv.Content.(*packets.Subscribe))
				if p, ok := t.responses[packets.SUBACK]; ok {
					p.(*packets.Suback).PacketID = recv.PacketID()
					if _, err := p.WriteTo(t.conn); err != nil {
						log.Println(err)
					}
				}
			case packets.UNSUBSCRIBE:
				log.Println("received unsubscribe", recv.Content.(*packets.Unsubscribe))
				if p, ok := t.responses[packets.UNSUBACK]; ok {
					p.(*packets.Unsuback).PacketID = recv.PacketID()
					if _, err := p.WriteTo(t.conn); err != nil {
						log.Println(err)
					}
				}
			case packets.AUTH:
			case packets.PUBLISH:
				log.Println("received publish", recv.Content.(*packets.Publish))
				switch recv.Content.(*packets.Publish).QoS {
				case 1:
					if p, ok := t.responses[packets.PUBACK]; ok {
						p.(*packets.Puback).PacketID = recv.PacketID()
						if _, err := p.WriteTo(t.conn); err != nil {
							log.Println(err)
						}
					}
				case 2:
					if p, ok := t.responses[packets.PUBREC]; ok {
						p.(*packets.Pubrec).PacketID = recv.PacketID()
						log.Println("sending pubrec")
						if _, err := p.WriteTo(t.conn); err != nil {
							log.Println(err)
						}
						log.Println("sent pubrec")
					}
				}

			case packets.PUBACK, packets.PUBCOMP, packets.SUBACK, packets.UNSUBACK:
			case packets.PUBREC:
			case packets.PUBREL:
				log.Println("received pubrel", recv.Content.(*packets.Pubrel))
				if p, ok := t.responses[packets.PUBCOMP]; ok {
					p.(*packets.Pubcomp).PacketID = recv.PacketID()
					if _, err := p.WriteTo(t.conn); err != nil {
						log.Println(err)
					}
				}
			case packets.DISCONNECT:
			case packets.PINGREQ:
				log.Println("test server sending pingresp")
				pr := packets.NewControlPacket(packets.PINGRESP)
				pr.WriteTo(t.conn)
			}
		}
	}
}
