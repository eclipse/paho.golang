package paho

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/eclipse/paho.golang/packets"
)

// PingFailHandler is a type for the function that is invoked
// when the sending of a PINGREQ failed or when we have sent
// a PINGREQ to the server and not received a PINGRESP within
// the appropriate amount of time.
type PingFailHandler func(error)

// Pinger is an interface of the functions for a struct that is
// used to manage sending PingRequests and responding to
// PingResponses.
// Start() takes a net.Conn which is a connection over which an
// MQTT session has already been established, and a time.Duration
// of the keepalive setting passed to the server when the MQTT
// session was established.
// Stop() is used to stop the Pinger
// PingResp() is the function that is called by the Client when
// a PINGRESP is received
// SetDebug() is used to pass in a Logger to be used to log debug
// information, for example sharing a logger with the main client
type Pinger interface {
	Start(net.Conn, time.Duration)
	Stop()
	PingResp()
	SetDebug(Logger)
}

// PingHandler is the library provided default Pinger
type PingHandler struct {
	sync.Mutex
	stop             chan struct{}
	pingRespReceived chan struct{}
	pingFailHandler  PingFailHandler
	debug            Logger
}

// DefaultPingerWithCustomFailHandler returns an instance of the
// default Pinger but with a custom PingFailHandler that is called
// when the client has not received a response to a PingRequest
// within the appropriate amount of time
func DefaultPingerWithCustomFailHandler(pfh PingFailHandler) *PingHandler {
	return &PingHandler{
		pingFailHandler: pfh,
		debug:           NOOPLogger{},
	}
}

// Start is the library provided Pinger's implementation of
// the required interface function()
func (p *PingHandler) Start(c net.Conn, pt time.Duration) {
	p.Lock()
	if p.stop != nil {
		select {
		case <-p.stop:
			// Stopped before, need to reset
		default:
			// Already running
			p.Unlock()
			return
		}
	}
	p.stop = make(chan struct{})
	p.pingRespReceived = make(chan struct{}, 1)
	p.Unlock()

	defer p.Stop()

	checkTicker := time.NewTicker(pt)
	defer checkTicker.Stop()

	sendPingreq := func() error {
		p.debug.Println("PingHandler sending ping request")
		_, err := packets.NewControlPacket(packets.PINGREQ).WriteTo(c)
		return err
	}
	if err := sendPingreq(); err != nil {
		p.pingFailHandler(err)
		return
	}

	for {
		select {
		case <-p.stop:
			return
		case <-checkTicker.C:
			select {
			case <-p.pingRespReceived:
				if err := sendPingreq(); err != nil {
					p.pingFailHandler(err)
					return
				}
			default:
				p.pingFailHandler(fmt.Errorf("ping resp timed out"))
				return
			}
		}
	}
}

// Stop is the library provided Pinger's implementation of
// the required interface function()
func (p *PingHandler) Stop() {
	p.Lock()
	defer p.Unlock()
	if p.stop == nil {
		return
	}

	select {
	case <-p.stop:
		//Already stopped, do nothing
	default:
		p.debug.Println("pingHandler stopping")
		close(p.stop)
	}
}

// PingResp is the library provided Pinger's implementation of
// the required interface function()
func (p *PingHandler) PingResp() {
	p.debug.Println("pingHandler received ping response")
	select {
	case p.pingRespReceived <- struct{}{}:
	default:
	}
}

// SetDebug is the library provided Pinger's implementation of
// the required interface function()
func (p *PingHandler) SetDebug(l Logger) {
	p.debug = l
}
