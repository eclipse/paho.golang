package paho

import (
	"log"
	"net"
	"sync"

	"github.com/eclipse/paho.golang/packets"
)

type fakeAuth struct{}

func (f *fakeAuth) Authenticate(a *Auth) *Auth {
	return &Auth{
		Properties: &AuthProperties{
			AuthMethod: "TEST",
			AuthData:   []byte("secret data"),
		},
	}
}

func (f *fakeAuth) Authenticated() {}

type testServer struct {
	conn       net.Conn
	clientConn net.Conn
	stop       chan struct{}
	responses  map[byte]packets.Packet

	receivedMu      sync.Mutex
	receivedPubacks []*packets.Puback
	receivedPubrecs []*packets.Pubrec
}

func newTestServer() *testServer {
	t := &testServer{
		stop:      make(chan struct{}),
		responses: make(map[byte]packets.Packet),
	}
	t.conn, t.clientConn = net.Pipe()

	return t
}

func (t *testServer) ClientConn() net.Conn {
	return t.clientConn
}

func (t *testServer) SetResponse(pt byte, p packets.Packet) {
	t.responses[pt] = p
}

func (t *testServer) SendPacket(p packets.Packet) error {
	_, err := p.WriteTo(t.conn)

	return err
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
				log.Println("received", recv.Content.(*packets.Connect))
				if p, ok := t.responses[packets.CONNACK]; ok {
					if _, err := p.WriteTo(t.conn); err != nil {
						log.Println(err)
					}
				}
			case packets.SUBSCRIBE:
				log.Println("received", recv.Content.(*packets.Subscribe))
				if p, ok := t.responses[packets.SUBACK]; ok {
					p.(*packets.Suback).PacketID = recv.PacketID()
					if _, err := p.WriteTo(t.conn); err != nil {
						log.Println(err)
					}
				}
			case packets.UNSUBSCRIBE:
				log.Println("received", recv.Content.(*packets.Unsubscribe))
				if p, ok := t.responses[packets.UNSUBACK]; ok {
					p.(*packets.Unsuback).PacketID = recv.PacketID()
					if _, err := p.WriteTo(t.conn); err != nil {
						log.Println(err)
					}
				}
			case packets.AUTH:
				log.Println("received", recv.Content.(*packets.Auth))
				if p, ok := t.responses[packets.AUTH]; ok {
					log.Println("sending auth")
					if _, err := p.WriteTo(t.conn); err != nil {
						log.Println(err)
					}
				}
			case packets.PUBLISH:
				log.Println("received", recv.Content.(*packets.Publish))
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

			case packets.PUBACK:
				log.Println("received", recv.Content.(*packets.Puback))
				t.receivedMu.Lock()
				t.receivedPubacks = append(t.receivedPubacks, recv.Content.(*packets.Puback))
				t.receivedMu.Unlock()
			case packets.PUBCOMP:
				log.Println("received", recv.Content.(*packets.Pubcomp))
			case packets.SUBACK:
				log.Println("received")
			case packets.UNSUBACK:
				log.Println("received")
			case packets.PUBREC:
				log.Println("received", recv.Content.(*packets.Pubrec))
				if p, ok := t.responses[packets.PUBREL]; ok {
					p.(*packets.Pubrel).PacketID = recv.PacketID()
					if _, err := p.WriteTo(t.conn); err != nil {
						log.Println(err)
					}
				}
				t.receivedMu.Lock()
				t.receivedPubrecs = append(t.receivedPubrecs, recv.Content.(*packets.Pubrec))
				t.receivedMu.Unlock()
			case packets.PUBREL:
				log.Println("received", recv.Content.(*packets.Pubrel))
				if p, ok := t.responses[packets.PUBCOMP]; ok {
					p.(*packets.Pubcomp).PacketID = recv.PacketID()
					if _, err := p.WriteTo(t.conn); err != nil {
						log.Println(err)
					}
				}
			case packets.DISCONNECT:
				log.Println("received")
			case packets.PINGREQ:
				log.Println("test server sending pingresp")
				pr := packets.NewControlPacket(packets.PINGRESP)
				if _, err := pr.WriteTo(t.conn); err != nil {
					log.Println("error writing pingreq", err)
				}
			}
		}
	}
}

func (t *testServer) ReceivedPubacks() []packets.Puback {
	t.receivedMu.Lock()
	defer t.receivedMu.Unlock()
	packets := make([]packets.Puback, len(t.receivedPubacks))
	for k := range t.receivedPubacks {
		packets[k] = *t.receivedPubacks[k]
	}
	return packets
}

func (t *testServer) ReceivedPubrecs() []packets.Pubrec {
	t.receivedMu.Lock()
	defer t.receivedMu.Unlock()
	packets := make([]packets.Pubrec, len(t.receivedPubrecs))
	for k := range t.receivedPubrecs {
		packets[k] = *t.receivedPubrecs[k]
	}
	return packets
}
