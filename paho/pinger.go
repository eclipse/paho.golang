/*
 * Copyright (c) 2024 Contributors to the Eclipse Foundation
 *
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Public License v2.0
 *  and Eclipse Distribution License v1.0 which accompany this distribution.
 *
 * The Eclipse Public License is available at
 *    https://www.eclipse.org/legal/epl-2.0/
 *  and the Eclipse Distribution License is available at
 *    http://www.eclipse.org/org/documents/edl-v10.php.
 *
 *  SPDX-License-Identifier: EPL-2.0 OR BSD-3-Clause
 */

package paho

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho/log"
)

type Pinger interface {
	// Run() starts the pinger. It blocks until the pinger is stopped.
	// If the pinger stops due to an error, it returns the error.
	// If the keepAlive is 0, it returns nil immediately.
	// Run() must be called only once.
	Run(conn net.Conn, keepAlive uint16) error

	// Stop() gracefully stops the pinger.
	Stop()

	// PacketSent() is called when a packet is sent to the server.
	PacketSent()

	// PingResp() is called when a PINGRESP is received from the server.
	PingResp()

	// SetDebug() sets the logger for debugging.
	// It is not thread-safe and must be called before Run() to avoid race conditions.
	SetDebug(log.Logger)
}

// DefaultPinger is the default implementation of Pinger.
type DefaultPinger struct {
	timer             *time.Timer
	keepAlive         uint16
	conn              net.Conn
	previousPingAcked chan struct{}
	done              chan struct{}
	errChan           chan error
	ackReceived       chan struct{}
	stopOnce          sync.Once
	mu                sync.Mutex
	debug             log.Logger
}

func NewDefaultPinger() *DefaultPinger {
	previousPingAcked := make(chan struct{}, 1)
	previousPingAcked <- struct{}{} // initial value
	return &DefaultPinger{
		previousPingAcked: previousPingAcked,
		errChan:           make(chan error, 1),
		done:              make(chan struct{}),
		ackReceived:       make(chan struct{}, 1),
		debug:             log.NOOPLogger{},
	}
}

func (p *DefaultPinger) Run(conn net.Conn, keepAlive uint16) error {
	if keepAlive == 0 {
		p.debug.Println("Run() returning immediately due to keepAlive == 0")
		return nil
	}
	if conn == nil {
		return fmt.Errorf("conn is nil")
	}
	p.mu.Lock()
	if p.timer != nil {
		p.mu.Unlock()
		return fmt.Errorf("Run() already called")
	}
	select {
	case <-p.done:
		p.mu.Unlock()
		return fmt.Errorf("Run() called after stop()")
	default:
	}
	p.keepAlive = keepAlive
	p.conn = conn
	p.timer = time.AfterFunc(0, p.sendPingreq) // Immediately send first pingreq
	p.mu.Unlock()

	return <-p.errChan
}

func (p *DefaultPinger) Stop() {
	p.stop(nil)
}

func (p *DefaultPinger) PacketSent() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.timer == nil {
		p.debug.Println("PacketSent() called before Run()")
		return
	}
	select {
	case <-p.done:
		p.debug.Println("PacketSent() returning due to done channel")
		return
	default:
	}

	p.debug.Println("PacketSent() resetting timer")
	p.timer.Reset(time.Duration(p.keepAlive) * time.Second)
}

func (p *DefaultPinger) PingResp() {
	select {
	case p.ackReceived <- struct{}{}:
	default:
		p.debug.Println("PingResp() called when ackReceived channel is full")
		p.stop(fmt.Errorf("received unexpected PINGRESP"))
	}
}

func (p *DefaultPinger) SetDebug(debug log.Logger) {
	p.debug = debug
}

func (p *DefaultPinger) sendPingreq() {
	// Wait for previous ping to be acked before sending another
	select {
	case <-p.previousPingAcked:
	case <-p.done:
		p.debug.Println("sendPingreq() returning before sending PINGREQ due to done channel")
		return
	}

	p.debug.Println("sendPingreq() sending PINGREQ packet")
	if _, err := packets.NewControlPacket(packets.PINGREQ).WriteTo(p.conn); err != nil {
		p.stop(fmt.Errorf("failed to send PINGREQ: %w", err))
		p.debug.Printf("sendPingreq() calling stop() and returning due to packet write error: %v", err)
		return
	}
	p.debug.Println("sendPingreq() sent PINGREQ packet, waiting for PINGRESP")
	pingrespTimeout := time.NewTimer(time.Duration(p.keepAlive) * time.Second)

	p.PacketSent()

	select {
	case <-p.done:
		p.debug.Println("sendPingreq() returning after sending PINGREQ due to done channel")
	case <-p.ackReceived:
		p.previousPingAcked <- struct{}{}
		p.debug.Println("sendPingreq() returning after receiving PINGRESP")
	case <-pingrespTimeout.C:
		p.debug.Println("sendPingreq() calling stop() and returning due to PINGRESP timeout")
		p.stop(fmt.Errorf("PINGRESP timed out"))
		return
	}

	// Stop the timer if it hasn't fired yet
	if !pingrespTimeout.Stop() {
		<-pingrespTimeout.C
	}
}

func (p *DefaultPinger) stop(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.debug.Printf("stop() called with error: %v", err)
	p.stopOnce.Do(func() {
		if p.timer != nil {
			p.timer.Stop()
		}
		p.errChan <- err
		close(p.done)
	})
}
