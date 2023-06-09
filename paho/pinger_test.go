package paho

import (
	"net"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/packets"
	"github.com/stretchr/testify/assert"
)

func TestPingerPingHandlerTimeout(t *testing.T) {
	fakeServerConn, fakeClientConn := net.Pipe()
	defer fakeServerConn.Close()
	defer fakeClientConn.Close()

	serverConnBuffer := make([]byte, 1024)

	go func() {
		// keep reading from fakeServerConn and discard the values. exit when fakeServerConn is closed.
		for {
			_, err := fakeServerConn.Read(serverConnBuffer)
			if err != nil {
				return
			}
		}
	}()

	pfhValues := make(chan error)
	pfh := func(e error) {
		pfhValues <- e
	}

	pingHandler := DefaultPingerWithCustomFailHandler(pfh)

	go func() {
		pingHandler.Start(fakeClientConn, time.Second)
	}()
	defer pingHandler.Stop()

	select {
	case <-time.After(time.Second * 3):
		t.Error("pingHandler did not timeout in time")
	case err := <-pfhValues:
		assert.EqualError(t, err, "ping resp timed out")
	}
}

func TestPingerPingHandlerSuccess(t *testing.T) {
	ts := newTestServer()
	clientConn := ts.ClientConn()
	go ts.Run()
	defer ts.Stop()

	pfhValues := make(chan error)
	pfh := func(e error) {
		pfhValues <- e
	}

	pingHandler := DefaultPingerWithCustomFailHandler(pfh)

	go func() {
		for {
			recv, err := packets.ReadPacket(clientConn)
			if err != nil {
				return
			}
			if recv.Type == packets.PINGRESP {
				pingHandler.PingResp()
			}
		}
	}()

	go func() {
		pingHandler.Start(ts.ClientConn(), time.Second*5)
	}()
	defer pingHandler.Stop()

	select {
	case <-time.After(time.Second * 30):
		// Pass
	case <-pfhValues:
		t.Error("pingFailHandler was called when it should not have been")
	}
}
