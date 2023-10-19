package basictestserver

import (
	"log"
	"net"
	"sync"

	"github.com/eclipse/paho.golang/packets"
)

// Logger mirrors paho.Logger
type Logger interface {
	Println(v ...interface{})
	Printf(format string, v ...interface{})
}

type TestServer struct {
	conn       net.Conn
	clientConn net.Conn
	stop       chan struct{}
	done       chan struct{}
	responses  map[byte]packets.Packet

	receivedMu      sync.Mutex
	receivedPubacks []*packets.Puback
	receivedPubrecs []*packets.Pubrec

	logger Logger
}

func New(logger Logger) *TestServer {
	t := &TestServer{
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
		responses: make(map[byte]packets.Packet),
		logger:    logger,
	}
	t.conn, t.clientConn = net.Pipe()
	if logger == nil {
		t.logger = log.Default()
	}
	return t
}

func (t *TestServer) ClientConn() net.Conn {
	return t.clientConn
}

func (t *TestServer) SetResponse(pt byte, p packets.Packet) {
	t.responses[pt] = p
}

func (t *TestServer) SendPacket(p packets.Packet) error {
	_, err := p.WriteTo(t.conn)

	return err
}

func (t *TestServer) Stop() {
	t.conn.Close()
	close(t.stop)
	<-t.done
}

func (t *TestServer) Run() {
	defer close(t.done)
	for {
		select {
		case <-t.stop:
			return
		default:
			recv, err := packets.ReadPacket(t.conn)
			if err != nil {
				t.logger.Println("error in test server reading packet", err)
				return
			}
			t.logger.Println("test server received a control packet:", recv.PacketType())
			switch recv.Type {
			case packets.CONNECT:
				t.logger.Println("received", recv.Content.(*packets.Connect))
				if p, ok := t.responses[packets.CONNACK]; ok {
					if _, err := p.WriteTo(t.conn); err != nil {
						t.logger.Println(err)
					}
				}
			case packets.SUBSCRIBE:
				t.logger.Println("received", recv.Content.(*packets.Subscribe))
				if p, ok := t.responses[packets.SUBACK]; ok {
					p.(*packets.Suback).PacketID = recv.PacketID()
					if _, err := p.WriteTo(t.conn); err != nil {
						t.logger.Println(err)
					}
				}
			case packets.UNSUBSCRIBE:
				t.logger.Println("received", recv.Content.(*packets.Unsubscribe))
				if p, ok := t.responses[packets.UNSUBACK]; ok {
					p.(*packets.Unsuback).PacketID = recv.PacketID()
					if _, err := p.WriteTo(t.conn); err != nil {
						t.logger.Println(err)
					}
				}
			case packets.AUTH:
				t.logger.Println("received", recv.Content.(*packets.Auth))
				if p, ok := t.responses[packets.AUTH]; ok {
					t.logger.Println("sending auth")
					if _, err := p.WriteTo(t.conn); err != nil {
						t.logger.Println(err)
					}
				}
			case packets.PUBLISH:
				t.logger.Println("received", recv.Content.(*packets.Publish))
				switch recv.Content.(*packets.Publish).QoS {
				case 1:
					if p, ok := t.responses[packets.PUBACK]; ok {
						p.(*packets.Puback).PacketID = recv.PacketID()
						if _, err := p.WriteTo(t.conn); err != nil {
							t.logger.Println(err)
						}
					}
				case 2:
					if p, ok := t.responses[packets.PUBREC]; ok {
						p.(*packets.Pubrec).PacketID = recv.PacketID()
						t.logger.Println("sending pubrec")
						if _, err := p.WriteTo(t.conn); err != nil {
							t.logger.Println(err)
						}
						t.logger.Println("sent pubrec")
					}
				}

			case packets.PUBACK:
				t.logger.Println("received", recv.Content.(*packets.Puback))
				t.receivedMu.Lock()
				t.receivedPubacks = append(t.receivedPubacks, recv.Content.(*packets.Puback))
				t.receivedMu.Unlock()
			case packets.PUBCOMP:
				t.logger.Println("received", recv.Content.(*packets.Pubcomp))
			case packets.SUBACK:
				t.logger.Println("received")
			case packets.UNSUBACK:
				t.logger.Println("received")
			case packets.PUBREC:
				t.logger.Println("received", recv.Content.(*packets.Pubrec))
				if p, ok := t.responses[packets.PUBREL]; ok {
					p.(*packets.Pubrel).PacketID = recv.PacketID()
					if _, err := p.WriteTo(t.conn); err != nil {
						t.logger.Println(err)
					}
				}
				t.receivedMu.Lock()
				t.receivedPubrecs = append(t.receivedPubrecs, recv.Content.(*packets.Pubrec))
				t.receivedMu.Unlock()
			case packets.PUBREL:
				t.logger.Println("received", recv.Content.(*packets.Pubrel))
				if p, ok := t.responses[packets.PUBCOMP]; ok {
					p.(*packets.Pubcomp).PacketID = recv.PacketID()
					if _, err := p.WriteTo(t.conn); err != nil {
						t.logger.Println(err)
					}
				}
			case packets.DISCONNECT:
				t.logger.Println("received")
			case packets.PINGREQ:
				t.logger.Println("test server sending pingresp")
				pr := packets.NewControlPacket(packets.PINGRESP)
				if _, err := pr.WriteTo(t.conn); err != nil {
					t.logger.Println("error writing pingreq", err)
				}
			}
		}
	}
}

func (t *TestServer) ReceivedPubacks() []packets.Puback {
	t.receivedMu.Lock()
	defer t.receivedMu.Unlock()
	ret := make([]packets.Puback, len(t.receivedPubacks))
	for k := range t.receivedPubacks {
		ret[k] = *t.receivedPubacks[k]
	}
	return ret
}

func (t *TestServer) ReceivedPubrecs() []packets.Pubrec {
	t.receivedMu.Lock()
	defer t.receivedMu.Unlock()
	ret := make([]packets.Pubrec, len(t.receivedPubrecs))
	for k := range t.receivedPubrecs {
		ret[k] = *t.receivedPubrecs[k]
	}
	return ret
}
