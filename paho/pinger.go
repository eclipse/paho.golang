package paho

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eclipse/paho.golang/packets"
)

// PingFailHandler is a type for the function that is invoked
// when we have sent a Pingreq to the server and not received
// a Pingresp within 1.5x our pingtimeout
type PingFailHandler func(error)

// Pinger is an interface of the functions for a struct that is
// used to manage sending PingRequests and responding to
// PingResponses
// Start() takes a net.Conn which is a connection over which an
// MQTT session has already been established, and a time.Duration
// of the keepalive setting passed to the server when the MQTT
// session was established.
// Stop() is used to stop the Pinger
// PingResp() is the function that is called by the Client when
// a PingResponse is received
type Pinger interface {
	Start(net.Conn, time.Duration)
	Stop()
	PingResp()
	SetDebug(Logger)
}

// PingHandler is the library provided default Pinger
type PingHandler struct {
	mu              sync.Mutex
	lastPing        time.Time
	conn            net.Conn
	stop            chan struct{}
	pingFailHandler PingFailHandler
	pingOutstanding int32
	debug           Logger
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
	p.mu.Lock()
	p.conn = c
	p.stop = make(chan struct{})
	p.mu.Unlock()
	checkTicker := time.NewTicker(pt / 4)
	defer checkTicker.Stop()
	for {
		select {
		case <-p.stop:
			return
		case <-checkTicker.C:
			if atomic.LoadInt32(&p.pingOutstanding) > 0 && time.Since(p.lastPing) > (pt+pt>>1) {
				p.pingFailHandler(fmt.Errorf("ping resp timed out"))
				//ping outstanding and not reset in 1.5 times ping timer
				return
			}
			if time.Since(p.lastPing) >= pt {
				//time to send a ping
				p.debug.Println("pingHandler sending ping request")
				if _, err := packets.NewControlPacket(packets.PINGREQ).WriteTo(p.conn); err != nil {
					if p.pingFailHandler != nil {
						p.pingFailHandler(err)
					}
					return
				}
			}
		}
	}
}

// Stop is the library provided Pinger's implementation of
// the required interface function()
func (p *PingHandler) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stop == nil {
		return
	}
	p.debug.Println("pingHandler stopping")
	select {
	case <-p.stop:
		//Already stopped, do nothing
	default:
		close(p.stop)
	}
}

// PingResp is the library provided Pinger's implementation of
// the required interface function()
func (p *PingHandler) PingResp() {
	p.debug.Println("pingHandler resetting pingOutstanding")
	atomic.StoreInt32(&p.pingOutstanding, 0)
}

// SetDebug sets the logger l to be used for printing debug
// information for the pinger
func (p *PingHandler) SetDebug(l Logger) {
	p.debug = l
}
